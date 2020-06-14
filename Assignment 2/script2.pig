data = 
	LOAD 'players.csv' 
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
	AS
    	(game_id:int,
        country:chararray,
        won:int,
        num_supply_centers:int,
        eliminated:int,
        start_turn:int,
        end_turn:int);

filtered = FILTER data BY won == 1;
grouped = GROUP filtered BY country;
counted = FOREACH grouped GENERATE group, COUNT($1);

DUMP counted;