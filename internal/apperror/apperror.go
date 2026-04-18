package apperror

type Code string

const (
	CodeNotFound   Code = "NOT_FOUND"
	CodeValidation Code = "VALIDATION_FAILED"
	CodeInternal   Code = "INTERNAL_ERROR"
	CodeDuplicate  Code = "DUPLICATE_ENTRY"
	CodeIngestion  Code = "INGESTION_ERROR"
)

type AppError struct {
	Code    Code
	Status  int
	Message string
	Cause   error
}

func (e *AppError) Error() string {
	if e == nil {
		return "<nil>"
	}

	if e.Cause != nil {
		return string(e.Code) + ": " + e.Message + " - " + e.Cause.Error()
	}

	return string(e.Code) + ": " + e.Message
}

func New(code Code, message string, status int, cause error) *AppError {
	return &AppError{
		Code:    code,
		Status:  status,
		Message: message,
		Cause:   cause,
	}
}
