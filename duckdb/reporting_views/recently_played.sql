CREATE VIEW recently_played AS
SELECT * 
FROM read_csv_auto('./data/recently_played.csv');