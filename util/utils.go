package util

// Intersects returns true if the given two maps intersect.
func Intersects(m map[string]interface{}, inputM map[string]interface{}) bool {
	for k, _ := range inputM {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

// KeyIntersects returns true if the key set of the given `m`
// and the string array `inputKeys` intersects.
func KeyIntersects(m map[string]interface{}, inputKeys []string) bool {
	for _, k := range inputKeys {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

// EqualKeySets returns true if the key set of the given `m` equals
// the given key string array `inputKeys`.
func EqualKeySets(m map[string]interface{}, inputKeys []string) bool {
	if len(m) != len(inputKeys) {
		return false
	}

	for _, ik := range inputKeys {
		_, isPresent := m[ik]
		if !isPresent {
			return false
		}
	}

	return true
}
