ALTER TABLE nation DROP CONSTRAINT nation_regionkey ;
ALTER TABLE supplier DROP CONSTRAINT supplier_nationkey ;
ALTER TABLE customer DROP CONSTRAINT customer_nationkey ;
ALTER TABLE partsupp DROP CONSTRAINT partsupp_partkey ;
ALTER TABLE partsupp DROP CONSTRAINT partsupp_suppkey ;
ALTER TABLE orders DROP CONSTRAINT order_custkey ;
ALTER TABLE lineitem DROP CONSTRAINT lineitem_orderkey ;
ALTER TABLE lineitem DROP CONSTRAINT lineitem_partkey ;
ALTER TABLE lineitem DROP CONSTRAINT lineitem_suppkey ;
ALTER TABLE lineitem DROP CONSTRAINT lineitem_partsuppkey ;

ALTER TABLE region DROP CONSTRAINT regionkey ;
ALTER TABLE nation DROP CONSTRAINT nationkey ;
ALTER TABLE supplier DROP CONSTRAINT suppkey ;
ALTER TABLE customer DROP CONSTRAINT custkey ;
ALTER TABLE part DROP CONSTRAINT partkey ;
ALTER TABLE partsupp DROP CONSTRAINT partsuppkey ;

ALTER TABLE orders DROP CONSTRAINT orderkey ;
ALTER TABLE lineitem DROP CONSTRAINT lineitemkey ;