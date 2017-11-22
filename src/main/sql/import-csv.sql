CREATE TABLE AccountingData1
(Regnskabspost varchar(50), CVR varchar(50), Value varchar(50),	Precision varchar(50), Dato varchar(50),
 Report_Id varchar(50),	PeriodeSlut	varchar(50), Koncern varchar(50), Offtidspunkt varchar(50));

COPY AccountingData1 FROM '/mnt/csv/AccountingNumbers.csv' DELIMITER ';' CSV;
