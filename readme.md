# SQLite multithreaded tests

This was a small project to test if it is possible to work with sqlite in a
multithreaded application.

## Usage

To view all available options.

```console
mult_sql --help
```

The test defaults to 4 worker thread but it is possbile to specify the wanted
amount (this will only affect the insertion). 

```console
mult_sql -w 10 insert
```
