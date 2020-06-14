data = 
	LOAD 'orders.csv' 
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
	AS
    	(game_id:int,
        unit_id:int,
        unit_order:chararray,
        location:chararray,
        target:chararray,
        target_dest:chararray,
        success:int,
        reason:chararray,
        turn_num:int);
        
locations = FOREACH data GENERATE location, target;
filtered = FILTER locations BY target == 'Holland';
grouped = GROUP filtered BY location;
counted = FOREACH grouped GENERATE $0, 'Holland', COUNT($1);
ordered = ORDER counted BY $0;

DUMP ordered;