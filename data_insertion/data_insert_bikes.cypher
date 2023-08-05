// Uniqueness constraints.
CREATE CONSTRAINT FOR (accident:AccidentId) REQUIRE accident.AccidentId IS UNIQUE;
CREATE CONSTRAINT FOR (vehicleNumber:VehicleNumbers) REQUIRE vehicleNumber.VehicleNumbers IS UNIQUE;
CREATE CONSTRAINT FOR (CasualtiesNumber:CasualtiesNumbers) REQUIRE CasualtiesNumber.CasualtiesNumbers IS UNIQUE;
CREATE CONSTRAINT FOR (speedLimit:SpeedLimit) REQUIRE speedLimit.SpeedLimit IS UNIQUE;
CREATE CONSTRAINT FOR (roadType:RoadType) REQUIRE roadType.RoadType IS UNIQUE;
CREATE CONSTRAINT FOR (lightConditions:LightConditions) REQUIRE lightConditions.LightConditions IS UNIQUE;
CREATE CONSTRAINT FOR (weatherConditions:WeatherConditions) REQUIRE weatherConditions.WeatherConditions IS UNIQUE;
CREATE CONSTRAINT FOR (roadConditions:RoadConditions) REQUIRE roadConditions.RoadConditions IS UNIQUE;
CREATE CONSTRAINT FOR (biker:Biker) REQUIRE biker.Biker IS UNIQUE;
CREATE CONSTRAINT FOR (gender:Gender) REQUIRE gender.Gender IS UNIQUE;
CREATE CONSTRAINT FOR (severity:Severity) REQUIRE severity.Severity IS UNIQUE;
CREATE CONSTRAINT FOR (ageGroup:AgeGroup) REQUIRE ageGroup.AgeGroup IS UNIQUE;
CREATE CONSTRAINT FOR (year:Year) REQUIRE year.Year IS UNIQUE;
CREATE CONSTRAINT FOR (month:Month) REQUIRE month.Month IS UNIQUE;
CREATE CONSTRAINT FOR (timeOfDay:TimeOfDay) REQUIRE timeOfDay.TimeOfDay IS UNIQUE;

Accident Nodes:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (accident:AccidentId {AccidentId: row.Accident_Index})
    ON CREATE SET accident.AccidentId = row.Accident_Index)',
  {batchSize: 500, parallel: false}
)

# of Vehicle Nodes:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (vehicleNumber:VehicleNumbers {VehicleNumbers: row.Number_of_Vehicles})

    ON CREATE SET vehicleNumber.VehicleNumbers = row.Number_of_Vehicles)',
  {batchSize: 500, parallel: false}
)

# of Casualties Nodes:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (CasualtiesNumber:CasualtiesNumbers {CasualtiesNumbers: row.Number_of_Casualties})

    ON CREATE SET CasualtiesNumber.CasualtiesNumbers = row.Number_of_Casualties)',
  {batchSize: 500, parallel: false}
)


Speed limit nodes:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (speedLimit:SpeedLimit {SpeedLimit: row.Speed_limit})

    ON CREATE SET speedLimit.SpeedLimit = row.Speed_limit)',
  {batchSize: 500, parallel: false}
)

Road type:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (roadType:RoadType {roadType: row.Road_type})

    ON CREATE SET roadType.RoadType = row.Road_type)',
  {batchSize: 500, parallel: false}
)


Light conditions:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (lightConditions:LightConditions {LightConditions: row.Light_conditions})

    ON CREATE SET lightConditions.LightConditions = row.Light_conditions)',
  {batchSize: 500, parallel: false}
)


Weather_conditions:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (weatherConditions:WeatherConditions {WeatherConditions: row.Weather_conditions})

    ON CREATE SET weatherConditions.WeatherConditions = row.Weather_conditions)',
  {batchSize: 500, parallel: false}
)


Road_conditions:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (roadConditions:RoadConditions {RoadConditions: row.Road_conditions})

    ON CREATE SET roadConditions.RoadConditions = row.Road_conditions)',
  {batchSize: 500, parallel: false}
)

Biker Node:

CREATE (:Biker {isBikerInvolved: true})
CREATE (:Biker {isBikerInvolved: false})

Gender Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (gender:Gender {Gender: row.Gender})

    ON CREATE SET gender.Gender = row.Gender)',
  {batchSize: 500, parallel: false}
)

Severity Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (severity:Severity {Severity: row.Severity})

    ON CREATE SET severity.Severity = row.Severity)',
  {batchSize: 500, parallel: false}
)
Age Group Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (ageGroup:AgeGroup {AgeGroup: row.Age_Grp})

    ON CREATE SET ageGroup.AgeGroup = row.Age_Grp)',
  {batchSize: 500, parallel: false}
)

Year Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (year:Year {Year: row.Year})
    ON CREATE SET year.Year = row.Year)',
  {batchSize: 500, parallel: false}
)

Month Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (month:Month {Month: row.Month})
    ON CREATE SET month.Month = row.Month)',
  {batchSize: 500, parallel: false}
)

Time of Day Node:

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'FOREACH (ignore IN CASE WHEN row IS NOT NULL THEN [1] ELSE [] END |
    MERGE (timeOfDay:TimeOfDay {TimeOfDay: row.time_of_day})
    ON CREATE SET timeOfDay.TimeOfDay = row.time_of_day)',
  {batchSize: 500, parallel: false}
)


RELATIONSHIPS

involved_vehicles

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (vehicleNumber:VehicleNumbers {VehicleNumbers: row.Number_of_Vehicles})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND vehicleNumber IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved_vehicles]->(vehicleNumber)
   )',
  {batchSize: 500, parallel: false}
)

Involved_casualties

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (CasualtiesNumber:CasualtiesNumbers {CasualtiesNumbers: row.Number_of_Casualties})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND CasualtiesNumber IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved_casualties]->(CasualtiesNumber)
   )',
  {batchSize: 500, parallel: false}
)

Occured_at_speed_limit

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (speedLimit:SpeedLimit {SpeedLimit: row.Speed_limit})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND speedLimit
 IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:occurred_at_speed_limit]->(speedLimit)
   )',
  {batchSize: 500, parallel: false}
)

Occured_at_road_type

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (roadType:RoadType {roadType: row.Road_type})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND roadType IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:occurred_at_road_type]->(roadType)
   )',
  {batchSize: 500, parallel: false}
)

Occurred_under_light_conditions

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (lightConditions:LightConditions {LightConditions: row.Light_conditions})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND lightConditions IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:occurred_under_light_conditions]->(lightConditions)
   )',
  {batchSize: 500, parallel: false}
)

Occurred_under_weather_conditions

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (weatherConditions:WeatherConditions {WeatherConditions: row.Weather_conditions})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND weatherConditions IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:occurred_under_weather_conditions]->(weatherConditions)
   )',
  {batchSize: 500, parallel: false}
)

Occurred_under_road_conditions

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (roadConditions:RoadConditions {RoadConditions: row.Road_conditions})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND roadConditions IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:occurred_under_road_conditions]->(roadConditions)
   )',
  {batchSize: 500, parallel: false}
)

Involved

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (biker:Biker {isBikerInvolved:row.Biker})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND biker
 IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved]->(biker)
   )',
  {batchSize: 500, parallel: false}
)

Involved_Gender

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (gender:Gender {Gender: row.Gender})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND gender IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved_gender]->(gender)
   )',
  {batchSize: 500, parallel: false}
)

Involved_Severity

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (severity:Severity {Severity: row.Severity})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND severity IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved_severity]->(severity)
   )',
  {batchSize: 500, parallel: false}
)

Involved_age_group

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (ageGroup:AgeGroup {AgeGroup: row.Age_Grp})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND ageGroup IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:involved_age_group]->(ageGroup)
   )',
  {batchSize: 500, parallel: false}
)

Occurred_at_year

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (year:Year {Year: row.Year})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND year IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:Occurred_at_year]->(Year)
   )',
  {batchSize: 500, parallel: false}
)

occurred_at_month

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (year:Year {Year: row.Year})
MATCH(month:Month {Month : row.Month})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND year IS NOT NULL THEN [1] ELSE [] END |
     MERGE (year)-[:occurred_at_month]->(month)
   )',
  {batchSize: 500, parallel: false}
)

Occurred_at_time_of_day

CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///output.csv" AS row RETURN row',
  'WITH row
   MATCH (accident:AccidentId {AccidentId: row.Accident_Index})
   MATCH (timeOfDay:TimeOfDay {TimeOfDay: row.time_of_day})
   FOREACH (ignore IN CASE WHEN accident IS NOT NULL AND timeOfDay IS NOT NULL THEN [1] ELSE [] END |
     MERGE (accident)-[:0ccurred_at_time_of_day]->(timeOfDay)
   )',
  {batchSize: 500, parallel: false}
)

