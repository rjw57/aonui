// Package aonui provides support for downloading subsets of the Global
// Forecast System runs in GRIB format.
package aonui

// Default fetch strategy
var DefaultFetchStrategy = FetchStrategy{
	MaximumRetries:  5,
	TrySleepSeconds: 30,
}

// The proposed 0.25 degree resolution GRIBs from the Global Forecast System (GFS).
var GFSQuarterDegreeDataset = DataSource{
	Root:           "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/para/",
	RunPattern:     `^gfs\.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})$`,
	DatasetPattern: `^gfs\.t(?P<runHour>\d{2})z\.(?P<typeId>pgrb2b?)\.0p25\.f(?P<fcstHour>\d+)$`,
	FetchStrategy:  DefaultFetchStrategy,
	MinDatasets:    146,
}

// The original 0.5 degree resolution GRIBs from the Global Forecast System (GFS).
var GFSHalfDegreeDataset = DataSource{
	Root:            "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/",
	RunPattern:      `^gfs\.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})$`,
	DatasetPattern:  `^gfs\.t(?P<runHour>\d{2})z.(?P<typeId>pgrb2b?f)(?P<fcstHour>\d+)$`,
	FetchStrategy:   DefaultFetchStrategy,
	MaxForecastHour: 200,
	MinDatasets:     186,
}
