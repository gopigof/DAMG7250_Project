// Customer -> PLACES -> Order
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (customer:Customer {Id: row.CustomerId})
   MATCH (order:Order {Id: row.OrderId})
   FOREACH (ignore IN CASE WHEN customer IS NOT NULL AND order IS NOT NULL THEN [1] ELSE [] END |
     MERGE (customer)-[:PLACES]-(order)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;

// Customer -> IS_PART_OF -> CustomerSegment
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (customer:Customer {Id: row.CustomerId})
   MATCH (cs:CustomerSegment {CustomerSegment: row.CustomerSegment})
   FOREACH (ignore IN CASE WHEN customer IS NOT NULL AND cs IS NOT NULL THEN [1] ELSE [] END |
     MERGE (customer)-[:IS_PART_OF]-(cs)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Customer -> FROM_CITY -> City
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (customer:Customer {Id: row.CustomerId})
   MATCH (cc:City {City: row.CustomerCity})
   FOREACH (ignore IN CASE WHEN customer IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (customer)-[:FROM_CITY]-(cc)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Customer | City -> OF_STATE -> State
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (state:State {State: row.CustomerState})
   MATCH (cc:City {City: row.CustomerCity})
   FOREACH (ignore IN CASE WHEN state IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (cc)-[:OF_STATE]-(state)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Customer | State -> OF_COUNTRY -> Country
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (state:State {State: row.CustomerState})
   MATCH (cc:Country {Country: row.CustomerCountry})
   FOREACH (ignore IN CASE WHEN state IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (state)-[:OF_COUNTRY]-(cc)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;

// Order -> IS_SHIPPED_FROM -> City
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (order:Order {Id: row.OrderId})
   MATCH (cc:City {City: row.OrderCity})
   FOREACH (ignore IN CASE WHEN order IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (order)-[:IS_SHIPPED_FROM]-(cc)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order | City -> OF_STATE -> State
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (state:State {State: row.OrderState})
   MATCH (cc:City {City: row.OrderCity})
   FOREACH (ignore IN CASE WHEN state IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (cc)-[:OF_STATE]-(state)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order | State -> OF_COUNTRY -> Country
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (state:State {State: row.OrderState})
   MATCH (cc:Country {Country: row.OrderCountry})
   FOREACH (ignore IN CASE WHEN state IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (state)-[:OF_COUNTRY]-(cc)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order | Country -> OF_MARKET -> Region
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (region:Region {Region: row.OrderRegion})
   MATCH (cc:Country {Country: row.OrderCountry})
   FOREACH (ignore IN CASE WHEN region IS NOT NULL AND cc IS NOT NULL THEN [1] ELSE [] END |
     MERGE (cc)-[:OF_MARKET]-(region)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order -> CONTAINS_MODE -> ShippingMode
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (order:Order {Id: row.OrderId})
   MATCH (sm:ShippingMode {ShippingMode: row.ShippingMode})
   FOREACH (ignore IN CASE WHEN order IS NOT NULL AND sm IS NOT NULL THEN [1] ELSE [] END |
     MERGE (order)-[:CONTAINS_MODE]-(sm)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order -> CONTAINS_STATUS -> DeliveryStatus
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (order:Order {Id: row.OrderId})
   MATCH (ds:DeliveryStatus {DeliveryStatus: row.DeliveryStatus})
   FOREACH (ignore IN CASE WHEN order IS NOT NULL AND ds IS NOT NULL THEN [1] ELSE [] END |
     MERGE (order)-[:CONTAINS_STATUS]-(ds)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Order -> CONTAINS -> Products
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (order:Order {Id: row.OrderId})
   MATCH (p:Product {Id: row.ProductCardId})
   FOREACH (ignore IN CASE WHEN order IS NOT NULL AND p IS NOT NULL THEN [1] ELSE [] END |
     MERGE (order)-[:CONTAINS]-(p)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Products -> BELONGS_TO_CATEGORY -> ProductCategory
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (pc:ProductCategory {Id: row.ProductCategoryId})
   MATCH (p:Product {Id: row.ProductCardId})
   FOREACH (ignore IN CASE WHEN pc IS NOT NULL AND p IS NOT NULL THEN [1] ELSE [] END |
     MERGE (p)-[:BELONGS_TO_CATEGORY]-(pc)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;


// Products -> BELONGS_TO_DEPARTMENT -> Department
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (d:Department {Id: row.DepartmentId})
   MATCH (p:Product {Id: row.ProductCardId})
   FOREACH (ignore IN CASE WHEN d IS NOT NULL AND p IS NOT NULL THEN [1] ELSE [] END |
     MERGE (p)-[:BELONGS_TO_DEPARTMENT]-(d)
  )',
  {batchSize: 500, parallel: true}
) YIELD *;
