CREATE VIEW artist_genre_plays AS
SELECT *
FROM read_parquet('./data/report_artist_genre_plays.parquet');