package validate

import "rndmcodeguy.in/relay/internal/respond"

type ValidationErrors []respond.FieldError

func (ve ValidationErrors) Error() string {
	if len(ve) == 0 {
		return "validation failed"
	}
	return ve[0].Field + ": " + ve[0].Message
}

func (ve ValidationErrors) Fields() []respond.FieldError {
	return []respond.FieldError(ve)
}

// helper so callers don't construct FieldError directly
func Field(field, message string) respond.FieldError {
	return respond.FieldError{Field: field, Message: message}
}
