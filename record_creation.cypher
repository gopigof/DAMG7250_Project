CREATE                                      (`Puerto Rico`:Country)<-[:OF_COUNTRY]-(`PR`:State)<-[:OF_STATE]-(`Caguas`:City)
                                              <-[:FROM_CITY]-(`73`:Customer {FirstName: 'Cally', LastName: 'Holloway',
                                                                             Latitude:  18.2514534,
                                                                             Longitude: -66.03705597,
                                                                             Street:    '5365 Noble Nectar Island',
                                                                             Zipcode:   725})
                                              -[:IS_PART_OF]->(:CustomerSegment),
  (`73`)-[:PLACES]->(`77202`:Order {PaymentType:           'Debit', RealShippingDays: 3,
                                    ScheduledShippingDays: 4, Benefit: 91.25, Sales: 314.61,
                                    OrderTimestamp:        '1/13/2018 12:06', Zipcode: 754,
                                    ShippingTimestamp:     '1/17/2018 12:06'})-[:IS_SHIPPED_FROM]->(`Beraski`:City)
                                              -[:OF_STATE]->(`Java Occidental`:State)-[:OF_COUNTRY]->(`Indonesia`:Country)-[:OF_MARKET]->(`Pacific Asia`:Market),
  (`Advance Delivery`:DeliveryStatus)<-[:CONTAINS_STATUS]-(`77202`)
                                              -[:CONTAINS]->(`1360`:Product {Name:'Smart Watch', Description:'desc', Image:'http:', Price:327.75, Category:'Sporting Goods'}) - [:BELONGS_TO_CATEGORY] -> (`Sporting Goods`:ProductCategory), (`77202`)- [:CONTAINS_MODE] ->(`Standard Class`:ShippingMode), (`1360`)- [:BELONGS_TO_DEPT] ->(`2`:Department {Name:'Fitness'})