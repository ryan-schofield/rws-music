CREATE VIEW track_plays AS
SELECT *
FROM read_parquet('./data/report_track_plays.parquet');