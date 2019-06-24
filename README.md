# dynocsv

[![Snap Status](https://build.snapcraft.io/badge/zshamrock/dynocsv.svg)](https://build.snapcraft.io/user/zshamrock/dynocsv)

Exports DynamoDB table into CSV

```
NAME:
   dynocsv - Export DynamoDB table into CSV file

USAGE:
   dynocsv
              --table/-t <table>
              [--columns/-c <comma separated columns>]
              [--output/-o <output file name>]

VERSION:
   1.0.0

AUTHOR:
   (c) Aliaksandr Kazlou

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --table value, -t value    table to export
   --columns value, -c value  optional columns to export from the table, if skipped, all columns will be exported
   --output value, -o value   output file, or the default <table name>.csv will be used
   --help, -h                 show help
   --version, -v              print the version
```

## Installation                                                                                                                                              
                                                                                                                                                             
Use the `go` command:                                                                                                                                        
                                                                                                                                                             
    $ go get github.com/zshamrock/dynocsv
    
Or using `snap`:                                                                                                                                             
                                                                                                                                                             
    $ snap install dynocsv                                                                                                                                   
                                                                                                                                                             
[![Get it from the Snap Store](https://snapcraft.io/static/images/badges/en/snap-store-black.svg)](https://snapcraft.io/dynocsv)

## Usage                                                                                                                                                     
                                                                                                                                                             
    $ AWS_PROFILE=<profile name> dynocsv -t <table name>     
    
## Copyright                                                                                                                                                 
                                                                                                                                                             
Copyright (C) 2019 by Aliaksandr Kazlou.                                                                                                                     
                                                                                                                                                             
dynocsv is released under MIT License.                                                                                                                       
See [LICENSE](https://github.com/zshamrock/dynocsv/blob/master/LICENSE) for details.      
