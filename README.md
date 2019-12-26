# dynocsv

Exports DynamoDB table into CSV [![Build Status](https://travis-ci.org/zshamrock/vmx.svg?branch=master)](https://travis-ci.org/zshamrock/dynocsv) [![dynocsv](https://snapcraft.io/dynocsv/badge.svg)](https://snapcraft.io/dynocsv)                                                                                                                                                    

```
NAME:
   dynocsv - Export DynamoDB table into CSV file

USAGE:
   dynocsv     
        --table/-t                                     <table> 
        [--columns/-c                                  <comma separated columns>] 
        [--skip-columns/-sc                            <comma separated columns to skip>] 
        [--limit/-l                                    <number>]
        [--profile/-p                                  <AWS profile>]
        [--index/-i                                    <index to query instead of table>]
        [--hash                                        <hash value>]
        [--sort                                        <sort value>]
        [--sort-[gt, ge, lt, le, begins-with, between] <sort value>]
        [--output/-o                                   <output file name>]

VERSION:
   1.1.1

AUTHOR:
   (c) Aliaksandr Kazlou

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --table value, -t value           table to export
   --index value, -i value           index to query if hash/sort are set instead of table (which is default)
   --columns value, -c value         columns to export from the table, if omitted, all columns will be exported (muttaly exclusive with "skip-columns")
   --skip-columns value, --sc value  columns skipped from export from the table, if omitted, all columns will be exported (muttaly exclusive with "columns")
   --limit value, -l value           limit number of records returned, if not set (i.e. 0) all items are fetched (default: 0)
   --profile value, -p value         AWS profile to use to connect to DynamoDB, otherwise the value from AWS_PROFILE env var is used if available, or then "default" if it is not set or empty
   --hash value                      Limit query by hash value (eq/=)
   --sort value                      Limit query by sort value (eq/=)
   --sort-gt value                   Limit query by sort value (gt/>)
   --sort-ge value                   Limit query by sort value (ge/>=)
   --sort-lt value                   Limit query by sort value (lt/<)
   --sort-le value                   Limit query by sort value (le/<=)
   --sort-begins-with value          Limit query by sort value (begins with)
   --sort-between value              Limit query by sort value (between), values are separated by comma, i.e. "value1,value2"
   --output value, -o value          output file, or the default <table name>.csv will be used
   --help, -h                        show help
   --version, -v                     print the version
```

Table of Contents
=================

* [Installation](#installation)
* [Usage](#usage)
* [AWS Connection](#aws-connection)
* [Query](#query)
* [Limits](#limits)

## Installation                                                                                                                                              
                                                                                                                                                             
Use the `go` command:                                                                                                                                        
                                                                                                                                                             
    $ go get github.com/zshamrock/dynocsv
    
Or using `snap`:                                                                                                                                             
                                                                                                                                                             
    $ snap install dynocsv
                                                                                                                                                             
[![Get it from the Snap Store](https://snapcraft.io/static/images/badges/en/snap-store-black.svg)](https://snapcraft.io/dynocsv)

*Note*: for Snap you would need explicitly allow `aws-config-credetials` plug which reads data from `$HOME/.aws` by 
running:
```
snap connect dynocsv:aws-config-credentials
```
otherwise it fails to read the credentials and will fail with 
> 019/10/24 17:34:14 MissingRegion: could not find region configuration
   panic: MissingRegion: could not find region configuration

## Usage                                                                                                                                                     
                                                                                                                                                             
    $ dynocsv -t <table name>
    
## AWS Connection

Connection to the AWS is established using profile credentials. There are 2 options to provide the AWS profile to use:

1. use explicit `--profile/-p` option to set the AWS profile to use, i.e. `dynocsv -p <profile name> -t <table name>`
2. set the env var `$AWS_PROFILE` before running the app, i.e. `AWS_PROFILE=<profile name> dynocsv -t <table name>`

If no explicit profile value is set, it looks for the env var `$AWS_PROFILE` if present or otherwise fallbacks to the `default` profile.

## Query

By default `Scan` operation is run to fetch all the data, if `--hash` and any of the `--sort` CLI arguments are provided,
`Query` operation will be run to query the corresponding data based on the key conditions specified by `--hash` and
`--sort` values. 

## Limits

Currently there are the following limitations:

- `String`, `Boolean`, `Number`, `Map`, `StringSet`, `NumberSet` and `List` data types are supported to export the data 
    from, attributes with other data type will still be present, but the value will be "" (empty string)
- there is no pause or proper throttling according to the current set table's RCU, so you might need manually to 
    increase the RCU value temporarily for the period of running the export
    
## Copyright                                                                                                                                                 
                                                                                                                                                             
Copyright (C) 2019 by Aliaksandr Kazlou.                                                                                                                     
                                                                                                                                                             
dynocsv is released under MIT License.                                                                                                                       
See [LICENSE](https://github.com/zshamrock/dynocsv/blob/master/LICENSE) for details.      
