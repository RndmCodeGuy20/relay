package rule

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
)

func EvaluateEvent(event InputEvent, rules []Rule) (EvaluationResult, error) {
	if event.Source == "" || event.EventType == "" {
		return EvaluationResult{}, errors.New("source and event_type are required")
	}
	if len(event.Payload) == 0 || !json.Valid(event.Payload) {
		return EvaluationResult{}, errors.New("payload must be valid JSON")
	}

	var payload any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return EvaluationResult{}, err
	}

	candidates := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if rule.Source != event.Source || rule.EventType != event.EventType {
			continue
		}
		if err := rule.Validate(); err != nil {
			continue
		}
		candidates = append(candidates, rule)
	}

	sortRules(candidates)

	result := EvaluationResult{Intents: make([]Intent, 0)}
	for _, rule := range candidates {
		if !matchesSelector(payload, rule.Selector) {
			continue
		}

		for _, target := range rule.Targets {
			result.Intents = append(result.Intents, Intent{
				RuleID:   rule.ID,
				RuleName: rule.Name,
				Target:   normalizeTarget(target),
			})
		}
	}

	result.NoMatch = len(result.Intents) == 0
	return result, nil
}

func sortRules(rules []Rule) {
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority > rules[j].Priority
		}
		return rules[i].ID.String() < rules[j].ID.String()
	})
}

func matchesSelector(payload any, selector Selector) bool {
	allPass := true
	if len(selector.All) > 0 {
		for _, predicate := range selector.All {
			if !matchesPredicate(payload, predicate) {
				allPass = false
				break
			}
		}
	}

	anyPass := true
	if len(selector.Any) > 0 {
		anyPass = false
		for _, predicate := range selector.Any {
			if matchesPredicate(payload, predicate) {
				anyPass = true
				break
			}
		}
	}

	return allPass && anyPass
}

func matchesPredicate(payload any, predicate Predicate) bool {
	actual, exists := resolvePath(payload, predicate.Path)

	switch predicate.Op {
	case OperatorExists:
		return exists
	case OperatorEq, OperatorNeq, OperatorIn, OperatorContains:
		if !exists {
			return false
		}
	default:
		return false
	}

	var expected any
	if err := json.Unmarshal(predicate.Value, &expected); err != nil {
		return false
	}

	switch predicate.Op {
	case OperatorEq:
		return reflect.DeepEqual(actual, expected)
	case OperatorNeq:
		return !reflect.DeepEqual(actual, expected)
	case OperatorIn:
		arr, ok := expected.([]any)
		if !ok {
			return false
		}
		for _, item := range arr {
			if reflect.DeepEqual(actual, item) {
				return true
			}
		}
		return false
	case OperatorContains:
		if actualString, ok := actual.(string); ok {
			expectedString, ok := expected.(string)
			if !ok {
				return false
			}
			return strings.Contains(actualString, expectedString)
		}
		if actualArray, ok := actual.([]any); ok {
			for _, item := range actualArray {
				if reflect.DeepEqual(item, expected) {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

func resolvePath(payload any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}

	current := payload
	for _, part := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, exists := obj[part]
		if !exists {
			return nil, false
		}
		current = next
	}

	return current, true
}

func normalizeTarget(target Target) Target {
	if target.Mode == "" {
		target.Mode = TargetModePublish
	}
	if target.Headers == nil {
		target.Headers = map[string]string{}
	}
	return target
}
