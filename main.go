package main

import (
	"bufio"
	"fmt"
	"github.com/zshamrock/dynocsv/aws/dynamodb"
	"gopkg.in/urfave/cli.v1"
	"log"
	"os"
	"strings"
)

const (
	tableFlagName          = "table"
	indexFlagName          = "index"
	columnsFlagName        = "columns"
	skipColumnsFlagName    = "skip-columns"
	limitFlagName          = "limit"
	profileFlagName        = "profile"
	hashFlagName           = "hash"
	sortFlagName           = "sort"
	sortGtFlagName         = "sort-gt"
	sortGeFlagName         = "sort-ge"
	sortLtFlagName         = "sort-lt"
	sortLeFlagName         = "sort-le"
	sortBeginsWithFlagName = "sort-begins-with"
	sortBetweenFlagName    = "sort-between"
	outputFlagName         = "output"

	sortBetweenValueSeparator = ","
)

var sortFlags = []string{
	sortFlagName,
	sortGtFlagName,
	sortGeFlagName,
	sortLtFlagName,
	sortLeFlagName,
	sortBeginsWithFlagName,
	sortBetweenFlagName,
}

const (
	appName = "dynocsv"
	version = "1.1.2"
)

func main() {
	app := cli.NewApp()
	app.Name = appName
	app.Usage = `Export DynamoDB table into CSV file`
	app.Version = version
	app.Author = "(c) Aliaksandr Kazlou"
	app.Metadata = map[string]interface{}{"GitHub": "https://github.com/zshamrock/dynocsv"}
	app.UsageText = fmt.Sprintf(`%s		 
        --table/-t                                     <table> 
        [--columns/-c                                  <comma separated columns>] 
        [--skip-columns/-sc                            <comma separated columns to skip>] 
        [--limit/-l                                    <number>]
        [--profile/-p                                  <AWS profile>]
        [--index/-i                                    <index to query instead of table>]
        [--hash                                        <hash value>]
        [--sort                                        <sort value>]
        [--sort-[gt, ge, lt, le, begins-with, between] <sort value>]
        [--output/-o                                   <output file name>]`,
		appName)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  fmt.Sprintf("%s, t", tableFlagName),
			Usage: "table to export",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s, i", indexFlagName),
			Usage: "index to query if hash/sort are set instead of table (which is default)",
		},
		cli.StringFlag{
			Name: fmt.Sprintf("%s, c", columnsFlagName),
			Usage: fmt.Sprintf(
				"columns to export from the table, if omitted, all columns will be exported "+
					"(muttaly exclusive with \"%s\")", skipColumnsFlagName),
		},
		cli.StringFlag{
			Name: fmt.Sprintf("%s, sc", skipColumnsFlagName),
			Usage: fmt.Sprintf(
				"columns skipped from export from the table, if omitted, all columns will be exported "+
					"(muttaly exclusive with \"%s\")", columnsFlagName),
		},
		cli.UintFlag{
			Name:  fmt.Sprintf("%s, l", limitFlagName),
			Usage: "limit number of records returned, if not set (i.e. 0) all items are fetched",
		},
		cli.StringFlag{
			Name: fmt.Sprintf("%s, p", profileFlagName),
			Usage: "AWS profile to use to connect to DynamoDB, otherwise the value from AWS_PROFILE env var is used " +
				"if available, or then \"default\" if it is not set or empty",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", hashFlagName),
			Usage: "Limit query by hash value (eq/=)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortFlagName),
			Usage: "Limit query by sort value (eq/=)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortGtFlagName),
			Usage: "Limit query by sort value (gt/>)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortGeFlagName),
			Usage: "Limit query by sort value (ge/>=)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortLtFlagName),
			Usage: "Limit query by sort value (lt/<)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortLeFlagName),
			Usage: "Limit query by sort value (le/<=)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortBeginsWithFlagName),
			Usage: "Limit query by sort value (begins with)",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s", sortBetweenFlagName),
			Usage: "Limit query by sort value (between), values are separated by comma, i.e. \"value1,value2\"",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%s, o", outputFlagName),
			Usage: "output file, or the default <table name>.csv will be used",
		},
	}
	app.Action = action

	err := app.Run(os.Args)
	if err != nil {
		log.Panicf("error encountered while running the app %v", err)
	}
}

func action(c *cli.Context) error {
	if len(os.Args) == 1 {
		cli.ShowAppHelpAndExit(c, 0)
	}
	table := mustFlag(c, tableFlagName)
	columns := c.String(columnsFlagName)
	skipColumns := c.String(skipColumnsFlagName)
	if columns != "" && skipColumns != "" {
		fmt.Printf("Both \"%s\" and \"%s\" are provided, they are mutually exclusive, please, use one.\n",
			columnsFlagName, skipColumnsFlagName)
		os.Exit(1)
	}
	filename := c.String(outputFlagName)
	if filename == "" {
		filename = fmt.Sprintf("%s.csv", table)
	}
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	limit := c.Uint(limitFlagName)
	profile := c.String(profileFlagName)
	hash := c.String(hashFlagName)
	qp := &dynamodb.QueryParams{}
	if hash != "" {
		qp.Hash = hash
		setSortFlags := make([]string, 0)
		for _, flag := range sortFlags {
			if c.String(flag) != "" {
				setSortFlags = append(setSortFlags, flag)
			}
		}
		if len(setSortFlags) > 1 {
			return fmt.Errorf("only single sort condition is supported, but found %d: %v", len(setSortFlags), setSortFlags)
		}
		if len(setSortFlags) != 0 {
			sortFlag := setSortFlags[0]
			switch sort := c.String(sortFlag); sortFlag {
			case sortFlagName:
				qp.Sort = sort
			case sortGtFlagName:
				qp.SortGt = sort
			case sortGeFlagName:
				qp.SortGe = sort
			case sortLtFlagName:
				qp.SortLt = sort
			case sortLeFlagName:
				qp.SortLe = sort
			case sortBeginsWithFlagName:
				qp.SortBeginsWith = sort
			case sortBetweenFlagName:
				qp.SortBetween = strings.Split(sort, sortBetweenValueSeparator)
			}
		}
	}
	headers := dynamodb.ExportToCSV(
		profile, table, c.String(indexFlagName), qp, columns, skipColumns, limit, bufio.NewWriter(file))
	if columns == "" {
		fmt.Println(strings.Join(headers, ","))
	}
	return file.Close()
}

func mustFlag(c *cli.Context, name string) string {
	value := c.String(name)
	if value == "" {
		log.Panic(fmt.Sprintf("%s is required", name))
	}
	return value
}
