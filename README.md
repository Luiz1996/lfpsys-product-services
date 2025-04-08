This is the Auth Service of LFPSys.

#### Main Technology Stack
* Java 21
* PostgreSQL
* Apache Kafka
* Redis
* Maven 3.9.1 (or higher)
* [Spring Boot Framework](https://spring.io/projects/spring-boot)

#### Communication Contracts
##### KAFKA
	Tópico: products
	Headers:
		client_id: <UUID>
	Payload:
		{
			"products": [
				{
					"name": "Mouse Gamer",
					"value": "300.00"
				},
				{
					"name": "Mousepad Gamer",
					"value": "100.00"
				}
			]
		}