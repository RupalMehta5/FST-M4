-- Load input file from HDFS
inputDialouges = LOAD 'hdfs:///user/rupal/inputs' USING PigStorage('\t') AS (name:chararray,line:chararray);

-- Filter out the first 2 lines
ranked = RANK inputDialouges;
OnlyDialouges = FILTER ranked BY (rank_inputDialouges > 2);

--Group by name
groupByName = Group OnlyDialouges BY name;

--Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

--Store results in HDFS
STORE namesOrdered INTO 'hdfs:///user/rupal/output/episodeIVOutput' USING PigStorage('\t');
STORE namesOrdered INTO 'hdfs:///user/rupal/output/episodeVOutput' USING PigStorage('\t');
STORE namesOrdered INTO 'hdfs:///user/rupal/output/episodeVIOutput' USING PigStorage('\t');
