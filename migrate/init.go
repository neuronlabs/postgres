package migrate

func init() {
	panicer := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	for _, dt := range defaultTypes {
		panicer(registerDataType(dt))
	}

	for _, dt := range defaultTypes {
		switch dt {
		case FSerial, FBigSerial:
			continue
		default:
			arr := &ArrayDataType{Subtype: dt}
			panicer(registerDataType(arr))
		}
	}
}
