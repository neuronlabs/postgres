package filters

// SplitFilterStrings splits the filter string value into array of strings.
func SplitFilterStrings(s string) []string {
	var (
		openCount int
		prevIndex int
		values    []string
	)

	for i, r := range s {
		switch r {
		case '(':
			openCount++
		case ')':
			openCount--
		case ',':
			if openCount == 0 {
				values = append(values, s[prevIndex:i])
				prevIndex = i + 1
			}
		}
	}

	values = append(values, s[prevIndex:])
	return values
}
