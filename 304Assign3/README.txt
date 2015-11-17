Compile Instructions
====================


Special Notes
=============
Assignment create and data files were renamed, if you want to use my program to 
load all the tables and tuples, you must rename them to create.txt and data.txt
and put them in the appropriate folder.

The command input is finicky, there can be no spaces before the instruction
itself, otherwise the read gets confused.
You are also not able to split queries into multiple lines for it to read correctly.

The user does not need to end the query with ";".

Assumptions:
- users enter full and valid SQL statements
- as no tables specify cascade on delete, I assume that is not required


Part 2
======
Input / Output Examples
-----------------------
Command: connect <username> <password>
Result: (Success) 
	Connected
	Statement created

Insert (Fail):
dbms> INSERT INTO ITEM VALUES('a00001',50,100,'y')
Entry was not added: duplicate

Insert (Success):
dbms> INSERT INTO ITEM VALUES('a09001',50,100,'y')
Entry successfully added


Deletion (Success):
dbms> INSERT INTO ITEM VALUES('a00091',50,0,'y')
Entry successfully added

dbms> delete item where upc = 'a00091'
Tuple deleted


Delete (Fail):
dbms> delete item where upc = 'a00001'
Stock is not empty, delete failed



Part 3
======
select distinct i.upc 
from purchase p, itemPurchase t, book b, item i
where t.upc = b.upc and i.upc = t.upc
and purchaseDate between to_date('15-OCT-25') and to_date('15-Oct-31') 
and t.quantity > 50 and b.flag_text = 'y'
and i.stock < 10

a00016 UPC
a00017 UPC


Part 4
======
select i.upc, SUM(t.quantity) as totalSales
from item i, itemPurchase t, purchase p
where t.upc = i.upc and t.t_id = p.t_id
and p.purchaseDate between to_date('15-OCT-25') and to_date('15-Oct-31') 
group by i.upc