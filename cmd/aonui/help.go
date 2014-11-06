package main

var helpTawhiri = &Command{
	UsageLine: "tawhiri",
	Short:     "\"Tawhiri order\" for data",
	Long: `
The Tawhiri predictor treats the wind data as a large five-dimensional array of
floating point values in a particular order.

The data should be ordered into a C-style array with dimensions forecast hour,
pressure, parameter, latitude and longitude. "C-style" here meaning that
adjacent records in the file correspond to changes in longitude.

Longitudes are ordered West-to-East and latitudes are ordered South-to-North.
Parameters are in the order HGT, UGRD, VGRD.  Pressures are in decreasing
numerical order. (This is so that the first pressure corresponds to the lowest
geo-potential height.) Forecast hours are in increasing numerical order.
`,
}
