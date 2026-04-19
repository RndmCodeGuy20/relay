package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/apperror"
	"rndmcodeguy.in/relay/internal/config"
	"rndmcodeguy.in/relay/internal/dispatch"
	"rndmcodeguy.in/relay/internal/event"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/stream"
)

type RelayService struct {
	conn           *pgconn.PgConn
	dispatcher     *dispatch.Dispatcher
	slotName       string
	currentLSN     pglogrepl.LSN
	outboxTable    string
	outboxSchema   string
	statusInterval int
	relations      map[uint32]relationInfo
}

type relationInfo struct {
	namespace string
	table     string
	columns   []string
}

func NewRelayService(ctx context.Context, s stream.Stream, pool *pgxpool.Pool, replicationDSN string, cfg config.RelayConfig) (*RelayService, error) {
	replConn, err := pgconn.Connect(ctx, replicationDSN)
	if err != nil {
		return nil, err
	}

	dispatcher := dispatch.New(cfg.Workers, s, pool, cfg.MaxRetries)
	outboxSchema, outboxTable := splitQualifiedTableName(cfg.OutboxTable)

	return &RelayService{
		conn:           replConn,
		dispatcher:     dispatcher,
		slotName:       cfg.SlotName,
		currentLSN:     0,
		outboxTable:    outboxTable,
		outboxSchema:   outboxSchema,
		statusInterval: cfg.StatusInterval,
		relations:      make(map[uint32]relationInfo),
	}, nil
}

func (s *RelayService) Run(ctx context.Context, startLSN pglogrepl.LSN) error {
	log := logger.FromContext(ctx)

	go func() {
		if err := s.dispatcher.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("dispatcher exited with error", zap.Error(err))
		}
	}()

	sysIdentify, err := pglogrepl.IdentifySystem(ctx, s.conn)
	if err != nil {
		return err
	}

	log.Info("Connected to PostgreSQL replication stream", zap.String("systemid", sysIdentify.SystemID), zap.Int32("timeline", sysIdentify.Timeline), zap.String("xlogpos", sysIdentify.XLogPos.String()), zap.String("dbname", sysIdentify.DBName))

	if startLSN == 0 {
		startLSN = sysIdentify.XLogPos
	}
	s.currentLSN = startLSN

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			"publication_names 'relay_pub'",
			"messages 'true'",
		},
	}
	if err := pglogrepl.StartReplication(ctx, s.conn, s.slotName, s.currentLSN, opts); err != nil {
		return err
	}

	log.Info("Started replication", zap.String("slot", s.slotName), zap.String("start_lsn", s.currentLSN.String()))
	standByDeadline := time.Now().Add(time.Duration(s.statusInterval) * time.Second)

	for {
		if time.Now().After(standByDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: s.currentLSN,
				WALFlushPosition: s.currentLSN,
				WALApplyPosition: s.currentLSN,
			}); err != nil {
				log.Error("Failed to send standby status update", zap.Error(err), zap.String("lsn", s.currentLSN.String()))
			} else {
				log.Debug("Sent standby status update", zap.String("lsn", s.currentLSN.String()))
			}
			standByDeadline = time.Now().Add(time.Duration(s.statusInterval) * time.Second)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, standByDeadline)
		rawMsg, err := s.conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			// log.Error("Failed to receive message", zap.Error(err), zap.String("lsn", s.currentLSN.String()))
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				log.Info("Context cancelled, shutting down relay service")
				return ctx.Err()
			}
			return err
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			return apperror.New(apperror.CodeInternal, fmt.Sprintf("received error from replication stream: %s", msg.Message), http.StatusInternalServerError, nil)

		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				log.Warn("received empty replication CopyData frame")
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Warn("keepalive parse error", zap.Error(err))
					continue
				}
				if pkm.ReplyRequested {
					standByDeadline = time.Now()
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Warn("xlogdata parse error", zap.Error(err))
					continue
				}
				s.currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

				ev, err := s.decodeWALMessage(xld.WALData, xld.WALStart)
				if err != nil {
					log.Warn("decode error", zap.Error(err), zap.String("lsn", xld.WALStart.String()))
					continue
				}
				if ev == nil {
					continue
				}

				if err := s.dispatcher.Submit(ctx, ev); err != nil {
					log.Error("failed to submit event to dispatcher", zap.Error(err), zap.String("event_id", ev.EventID.String()))
					continue
				}

				result := <-ev.Done
				if result != nil {
					log.Debug("event delivery failed, moved to DLQ", zap.String("event_id", ev.EventID.String()))
					continue
				}
				s.currentLSN = ev.LSN
			}
		default:
			continue
		}
	}
}

func (s *RelayService) decodeWALMessage(walData []byte, lsn pglogrepl.LSN) (*event.RelayEvent, error) {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return nil, fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		s.cacheRelationV2(m)
		return nil, nil
	case *pglogrepl.RelationMessage:
		s.cacheRelation(m)
		return nil, nil
	case *pglogrepl.InsertMessageV2:
		return s.decodeInsertTuple(m.RelationID, m.Tuple, lsn)
	case *pglogrepl.InsertMessage:
		return s.decodeInsertTuple(m.RelationID, m.Tuple, lsn)
	default:
		return nil, nil
	}
}

func (s *RelayService) cacheRelation(msg *pglogrepl.RelationMessage) {
	columns := make([]string, len(msg.Columns))
	for i, c := range msg.Columns {
		columns[i] = c.Name
	}
	s.relations[msg.RelationID] = relationInfo{namespace: msg.Namespace, table: msg.RelationName, columns: columns}
}

func (s *RelayService) cacheRelationV2(msg *pglogrepl.RelationMessageV2) {
	columns := make([]string, len(msg.Columns))
	for i, c := range msg.Columns {
		columns[i] = c.Name
	}
	s.relations[msg.RelationID] = relationInfo{namespace: msg.Namespace, table: msg.RelationName, columns: columns}
}

func (s *RelayService) decodeInsertTuple(relationID uint32, tuple *pglogrepl.TupleData, lsn pglogrepl.LSN) (*event.RelayEvent, error) {
	if tuple == nil {
		return nil, nil
	}

	rel, ok := s.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("missing relation metadata for relation id %d", relationID)
	}
	if rel.namespace != s.outboxSchema || rel.table != s.outboxTable {
		return nil, nil
	}

	outboxID, ok, err := readTupleColumn(tuple, rel.columns, "id")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("outbox insert missing required id column")
	}

	eventID, err := uuid.Parse(outboxID)
	if err != nil {
		return nil, fmt.Errorf("invalid outbox id %q: %w", outboxID, err)
	}

	eventType, _, err := readTupleColumn(tuple, rel.columns, "event_type")
	if err != nil {
		return nil, err
	}
	source, _, err := readTupleColumn(tuple, rel.columns, "source")
	if err != nil {
		return nil, err
	}

	sequence := int64(0)
	if sequenceText, hasSequence, seqErr := readTupleColumn(tuple, rel.columns, "sequence"); seqErr != nil {
		return nil, seqErr
	} else if hasSequence {
		parsed, parseErr := strconv.ParseInt(sequenceText, 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid sequence value %q: %w", sequenceText, parseErr)
		}
		sequence = parsed
	}

	return &event.RelayEvent{
		EventID:    eventID,
		RelayID:    uuid.New(),
		EventType:  eventType,
		Source:     source,
		LSN:        lsn,
		Sequence:   sequence,
		ReceivedAt: time.Now().UTC(),
		Done:       make(chan error, 1),
		Deadline:   time.Now().Add(30 * time.Second),
		Attempt:    0,
	}, nil
}

func readTupleColumn(tuple *pglogrepl.TupleData, columns []string, name string) (string, bool, error) {
	idx := -1
	for i, col := range columns {
		if col == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return "", false, nil
	}
	if idx >= len(tuple.Columns) {
		return "", false, fmt.Errorf("tuple is missing expected column %q", name)
	}
	value := tuple.Columns[idx]
	if value == nil {
		return "", false, nil
	}

	switch value.DataType {
	case pglogrepl.TupleDataTypeText:
		return string(value.Data), true, nil
	case pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("unsupported tuple data type %q for column %q", value.DataType, name)
	}
}

func splitQualifiedTableName(table string) (schema string, name string) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "public", "outbox_events"
	}
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 1 {
		return "public", parts[0]
	}
	return parts[0], parts[1]
}
