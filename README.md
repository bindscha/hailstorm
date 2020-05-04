# Hailstorm

Scala implementation of Hailstorm: Disaggregated Compute and Storage for Distributed LSM-based Databases.

## What is it?

If you have to ask, you probably should read [the paper](https://binds.ch/wp-content/uploads/2020/01/hailstorm2020.pdf) first.

Note: this is the filesystem part of Hailstorm. You will also need the modified RocksDB to run the experiments in the paper.

## Requirements

* Scala
* SBT
* Up-to-date JVM

## Getting started

Clone the project locally:

   ```$ git clone https://github.com/bindscha/hailstorm.git```

Build:

   ```$ sbt compile```

Compilation can take a while as SBT fetches all necessary dependencies.

Run:

   ```$ sbt run```

To specify CLI options:

   ```
   $ sbt
   sbt:hailstorm> run <options>
   ```

Available CLI options:

   ```
   -b, --blacklist  <arg>...      Blacklist matching files and folders
   -p, --blacklist-path  <arg>    Where to put blacklisted files (should be outside HailstormFS mountpoint)
   -i, --clear-on-init            Drop persistent file mapping at startup
   -c, --client                   Run client
   -d, --daemon                   Run daemon
   -f, --file-mapping-db  <arg>   Where to put the persistent file mapping database
   -o, --fuse-opts  <arg>...      FUSE options
   --me  <arg>                    Node ID
   -m, --mountpoint  <arg>        Where to mount the filesystem
   -l, --offloading               Enable Hailstorm Agent for compaction  offloading
   -v, --verbose                  Enable debug logging
   -h, --help                     Show help message
   --version                      Show version of this program
   ```

## License

Please see the file called [LICENSE.md](LICENSE.md).

## Contact
- Laurent Bindschaedler <laurent.bindschaedler@epfl.ch>

