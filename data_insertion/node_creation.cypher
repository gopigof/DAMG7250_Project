// Uniqueness constraints.
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Order) REQUIRE n.Id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (d:DeliveryStatus) REQUIRE d.DeliveryStatus IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:ShippingMode) REQUIRE s.ShippingMode IS UNIQUE;

CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.Id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (pc:ProductCategory) REQUIRE pc.Id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (d:Department) REQUIRE d.Id IS UNIQUE;

CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.Id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (cs:CustomerSegment) REQUIRE cs.CustomerSegment IS UNIQUE;

CREATE CONSTRAINT IF NOT EXISTS FOR (c:City) REQUIRE c.City IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:State) REQUIRE s.State IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Country) REQUIRE c.Country IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (r:Region) REQUIRE r.Region IS UNIQUE;

//------------------------------------------------------------------------

// Order Node
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
     MERGE (order:Order {Id: row.OrderId})
    ON CREATE SET
        order.PaymentType = row.PaymentType,
        order.RealShippingDays = row.RealShippingDays,
        order.ScheduledShippingDays = row.ScheduledShippingDays,
        order.Benefit = row.Benefit,
        order.OrderTimestamp = row.OrderTimestamp,
        order.ShippingTimestamp = row.ShippingTimestamp
 )', {batchSize: 500, parallel: true}
) YIELD *;


// of product Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (product:Product {Id: row.ProductCardId})
      ON CREATE SET
        product.Name = row.ProductName)
', {batchSize: 500, parallel: true}
) YIELD *;


// of productCategory Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (pc:ProductCategory {Id: row.ProductCategoryId})
    ON CREATE SET
        pc.Name = row.ProductCategoryName
    )', {batchSize: 500, parallel: true}
) YIELD *;


// of ShippingMode Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
  MERGE (shippingMode:ShippingMode {ShippingMode: row.ShippingMode})
  ON CREATE SET shippingMode.ShippingMode = row.ShippingMode)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Department Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (dept:Department {Id: row.DepartmentId})
      ON CREATE SET
        dept.Name = row.DepartmentName
    )', {batchSize: 500, parallel: true}
) YIELD *;


// DeliveryStatus Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (ds:DeliveryStatus {DeliveryStatus: row.DeliveryStatus})
      ON CREATE SET
        ds.DeliveryStatus = row.DeliveryStatus)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Customer Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (c:Customer {Id: row.CustomerId})
      ON CREATE SET
        c.FirstName = row.CustomerFname,
        c.LastName = row.CustomerLname,
        c.Latitude = row.Latitude,
        c.Longitude = row.Longitude
    )', {batchSize: 500, parallel: true}
) YIELD *;


//Customer city Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (custCity:City {City: row.CustomerCity})
      ON CREATE SET custCity.City = row.CustomerCity)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Customer State Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (custState:State {State: row.CustomerState})
      ON CREATE SET custState.State = row.CustomerState)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Customer Country Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (custCountry:Country {Country: row.CustomerCountry})
      ON CREATE SET custCountry.Country = row.CustomerCountry)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Customer Segment Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (custSegment:CustomerSegment {CustomerSegment: row.CustomerSegment})
      ON CREATE SET custSegment.CustomerSegment = row.CustomerSegment)',
 {batchSize: 500, parallel: true}
) YIELD *;


//Order city Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (orderCity:City {City: row.OrderCity})
      ON CREATE SET orderCity.City = row.OrderCity)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Order State Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (orderState:State {State: row.OrderState})
      ON CREATE SET orderState.State = row.OrderState)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Order Country Node:
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (orderCountry:Country {Country: row.OrderCountry})
      ON CREATE SET orderCountry.Country = row.OrderCountry)',
 {batchSize: 500, parallel: true}
) YIELD *;


// Order Market or Region
CALL apoc.periodic.iterate(
 'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
 'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
      MERGE (orderRegion:Region {Region: row.OrderRegion})
      ON CREATE SET orderRegion.Region = row.OrderRegion)',
 {batchSize: 500, parallel: true}
) YIELD *;
