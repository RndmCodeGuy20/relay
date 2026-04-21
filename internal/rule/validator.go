package rule

import (
	"encoding/json"
	"fmt"

	"rndmcodeguy.in/relay/internal/validate"
)

func (r Rule) Validate() error {
	var errs validate.ValidationErrors

	if r.Name == "" {
		errs = append(errs, validate.Field("name", "is required"))
	}
	if r.Source == "" {
		errs = append(errs, validate.Field("source", "is required"))
	}
	if r.EventType == "" {
		errs = append(errs, validate.Field("event_type", "is required"))
	}
	if r.Version <= 0 {
		errs = append(errs, validate.Field("version", "must be greater than 0"))
	}

	if r.Enabled && len(r.Targets) == 0 {
		errs = append(errs, validate.Field("targets", "must contain at least one target for enabled rules"))
	}

	targetNames := map[string]struct{}{}
	for i, target := range r.Targets {
		fieldPrefix := fmt.Sprintf("targets[%d]", i)
		if target.Name == "" {
			errs = append(errs, validate.Field(fieldPrefix+".name", "is required"))
		}
		if target.Subject == "" {
			errs = append(errs, validate.Field(fieldPrefix+".subject", "is required"))
		}
		if target.Mode != "" && target.Mode != TargetModePublish {
			errs = append(errs, validate.Field(fieldPrefix+".mode", "must be publish when provided"))
		}
		if _, exists := targetNames[target.Name]; target.Name != "" && exists {
			errs = append(errs, validate.Field(fieldPrefix+".name", "must be unique within a rule"))
		}
		targetNames[target.Name] = struct{}{}
	}

	for i, predicate := range r.Selector.All {
		validatePredicate(&errs, fmt.Sprintf("selector.all[%d]", i), predicate)
	}
	for i, predicate := range r.Selector.Any {
		validatePredicate(&errs, fmt.Sprintf("selector.any[%d]", i), predicate)
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func validatePredicate(errs *validate.ValidationErrors, fieldPrefix string, p Predicate) {
	if p.Path == "" {
		*errs = append(*errs, validate.Field(fieldPrefix+".path", "is required"))
	}

	switch p.Op {
	case OperatorEq, OperatorNeq, OperatorIn, OperatorContains, OperatorExists:
	default:
		*errs = append(*errs, validate.Field(fieldPrefix+".op", "must be one of eq, neq, in, contains, exists"))
	}

	if p.Op != OperatorExists {
		if len(p.Value) == 0 {
			*errs = append(*errs, validate.Field(fieldPrefix+".value", "is required"))
			return
		}
		if !json.Valid(p.Value) {
			*errs = append(*errs, validate.Field(fieldPrefix+".value", "must be valid JSON"))
		}
	}
}
