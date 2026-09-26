# userservice — EXPLAINED

## What it does (the whole point)

`userservice` keeps the **user profile** (name, email, phone, profile pic).
It does **not** handle passwords or login — that's `authservice`.
It learns about new users by **listening to Kafka**: when `authservice` creates an
account it publishes a "new user" message, and `userservice` picks it up and saves a
profile row. It also exposes two small HTTP endpoints to read/write a profile directly.

So this service is mostly a **Kafka consumer** with a database behind it.

---

## Jargon, explained once

| Term | Plain meaning |
|---|---|
| **Kafka** | A message queue. One service drops a message on a "topic", others read it later. Decouples services — the sender doesn't call the receiver directly. |
| **topic** | A named channel in Kafka. Here it's `user_service`. |
| **consumer group** | A label (`userinfo-consumer-group`) Kafka uses to track "how far this app has read". Restart the app → it resumes, doesn't re-read everything. |
| **producer / consumer** | Producer = writes messages (authservice). Consumer = reads them (userservice). |
| **serializer / deserializer** | Kafka only moves raw bytes. Serializer = object → bytes (authservice side). Deserializer = bytes → object (userservice side). |
| **`@Service` / `@RestController` / `@Repository` / `@Configuration`** | "Spring, create one of these and manage it for me." They're all Spring-managed objects ("beans"); the label just says what role it plays. |
| **`@Autowired`** | "Spring, inject the object I need here." You don't `new` it yourself. |
| **`@Bean`** | A method inside a `@Configuration` class whose return value becomes a Spring-managed object. |
| **`@KafkaListener`** | Marks a method Spring should call automatically for every message on a topic. |
| **Entity** | A Java class mapped to a DB table (`@Entity` + `@Table`). One object = one row. |
| **DTO** (Data Transfer Object) | A plain class for data crossing a boundary (HTTP body, Kafka message). Kept separate from the Entity so the wire format and the DB schema can change independently. |
| **`Optional<T>`** | A box that holds a value *or* nothing. Forces you to handle the "not found" case instead of hitting a `NullPointerException`. |
| **JPA / Hibernate** | JPA is the "save Java objects to SQL" standard; Hibernate is the implementation Spring uses. |
| **`ddl-auto`** | Whether Hibernate creates/alters DB tables on startup. `create` = **drop and recreate every start** (data lost). |
| **Lombok** (`@Getter`, `@Builder`, `@RequiredArgsConstructor`, …) | Generates boilerplate (getters, constructors, builder pattern) at compile time so you don't type it. |
| **snake_case naming strategy** | JSON uses `first_name`, Java uses `firstName`. `@JsonNaming(SnakeCaseStrategy)` bridges the two automatically. |

---

## Request flow — trace one new user end to end

```
authservice: POST /auth/v1/signup
   └─ saves user + password in authservice DB
   └─ publishes a JSON message to Kafka topic "user_service"
        { user_id, first_name, last_name, phone_number, email }
                     │
                     ▼   (Kafka holds the message until someone reads it)
─────────────────────────────────────────────────────────────────────
userservice
 1. Spring Kafka sees a message on "user_service" (config: application.properties).
 2. It runs the KEY through StringDeserializer, and the VALUE through
    UserInfoDeserializer  (deserializer/UserInfoDeserializer.java)
       → Jackson ObjectMapper turns the JSON bytes into a UserInfoDto object.
 3. Spring calls  AuthServiceConsumer.listen(UserInfoDto eventData)
       (consumer/AuthServiceConsumer.java)   ← the @KafkaListener method
 4. listen() calls  userService.createOrUpdateUser(eventData)
       (service/UserService.java)
 5. UserService:
       a. userRepository.findByUserId(id)      → Optional<UserInfo>   (SELECT)
       b. present? save the new values (UPDATE).  absent? save a new row (INSERT).
          both go through  userInfoDto.transformToUserInfo()  (DTO → Entity)
          then  userRepository.save(entity)     (repository/UserRepository.java)
 6. Row is now in the `users` table of the userservice DB. Done — no response,
    Kafka consumption is fire-and-forget.
```

**HTTP path (no Kafka):** `UserController` (`consumer/UserController.java`) →
`UserService.getUser` / `createOrUpdateUser` → `UserRepository` → DB. Same service
layer, different entry point.

---

## Every file, one line

| File | Role |
|---|---|
| `UserserviceApplication.java` | Entry point. `@SpringBootApplication` = "scan this package, autoconfigure everything, start the web server". |
| `config/UserServiceConfig.java` | Defines one `@Bean`: a shared Jackson `ObjectMapper` (JSON ↔ Java). |
| `consumer/AuthServiceConsumer.java` | The Kafka listener. Receives `UserInfoDto` events and forwards to `UserService`. **(TASK 1)** |
| `consumer/UserController.java` | HTTP endpoints: `GET /user/v1/getUser`, `POST /user/v1/createUpdate`, `GET /health`. **(TASK 5)** |
| `deserializer/UserInfoDeserializer.java` | Kafka bytes → `UserInfoDto` using `ObjectMapper`. Referenced from `application.properties`. |
| `entities/UserInfo.java` | `@Entity` — the `users` table. What actually gets stored. |
| `entities/UserInfoDto.java` | The wire/transfer shape + `transformToUserInfo()` to convert to the entity. **(TASK 3)** |
| `repository/UserRepository.java` | DB access. Extends `CrudRepository` (gives `save`, `findById`, …) + one custom finder. **(TASK 2)** |
| `service/UserService.java` | Business logic: upsert a user, fetch a user. Sits between controller/consumer and repository. **(TASK 4)** |
| `application.properties` | All config (below). |
| `Dockerfile` | Packages the built JAR into a container image on `openjdk:21`. |
| `build.gradle` | Dependencies + Java 21 + main class. |

---

## Every config value (`src/main/resources/application.properties`)

| Line | What it controls | Why this value |
|---|---|---|
| `spring.kafka.bootstrap-servers=${KAFKA_HOST:localhost}:${KAFKA_PORT:9092}` | Address of the Kafka broker to connect to | `${VAR:default}` = env var or fallback. **Must point at the same broker authservice writes to** (see "wired to your machine"). |
| `spring.kafka.consumer.properties.max.poll.interval.ms=300000` | Max 5 min between polls before Kafka assumes this consumer is dead and rebalances | Generous, so slow DB writes don't get the consumer kicked out |
| `spring.kafka.consumer.properties.session.timeout.ms=45000` | Heartbeat timeout — 45s of silence = consumer considered gone | Kafka default-ish; fine |
| `spring.kafka.consumer.auto-offset-reset=earliest` | On first run with no saved position, start from the **oldest** message | So you don't miss users created before userservice first started |
| `spring.kafka.consumer.group-id=userinfo-consumer-group` | This app's consumer-group name (offset tracking) | Any stable string; don't change it casually or it re-reads from `earliest` |
| `spring.kafka.consumer.key-deserializer=...StringDeserializer` | Message **key** bytes → String | authservice sends a String key |
| `spring.kafka.consumer.value-deserializer=...UserInfoDeserializer` | Message **value** bytes → `UserInfoDto` | Points at the custom class in `deserializer/` |
| `spring.kafka.consumer.properties.spring.json.trusted.packages=*` | Allow deserializing into any package | `*` = trust all (fine locally; tighten in prod) |
| `spring.kafka.topic-json.name=user_service` | The topic name | **Must exactly match** authservice's `spring.kafka.topic-json.name` |
| `spring.kafka.consumer.properties.spring.json.type.mapping=auth:...UserInfoDto` | Maps a short type name in the message header to a class | Unused in practice here — the custom deserializer ignores headers and always builds `UserInfoDto`. Harmless. |
| `spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver` | Which JDBC driver | MySQL |
| `spring.datasource.url=jdbc:mysql://${MYSQL_HOST:mysql}:${MYSQL_PORT:3306}/${MYSQL_DB:userservice}?...` | DB host / port / schema + connection flags | Default host `mysql` only works inside docker-compose; **override locally** |
| `spring.datasource.username=${MYSQL_USER:root}` / `password=${MYSQL_PASSWORD:password}` | DB login | Tutorial's docker defaults — change to your real MySQL creds |
| `...url flags: useSSL=false, allowPublicKeyRetrieval=true, characterEncoding=UTF-8` | Skip TLS (local), allow MySQL 8 auth handshake over plain, use UTF-8 | Local-dev convenience; not for production |
| `spring.jpa.show-sql=true` | Log every SQL statement | Learning/debugging |
| `spring.jpa.hibernate.ddl-auto=create` | **Drops and recreates all tables on every startup** | Tutorial convenience. Data is wiped each restart. Change to `update` once your schema is stable and you want to keep data. |
| `server.port=9810` | HTTP port this service listens on | authservice uses 9898; pick anything free |
| `spring.datasource.hikari.maximum-pool-size=20` / `minimum-idle=10` | DB connection pool size (max open / kept-warm) | Sane small-app defaults |
| `logging.level.*=DEBUG` | Verbose logs for security / kafka | Learning; set to `INFO` later |

---

## Things that talk to other systems — and what breaks if wrong

| Talks to | Via | If misconfigured |
|---|---|---|
| **Kafka broker** | `spring.kafka.bootstrap-servers`, topic `user_service`, group `userinfo-consumer-group` | Wrong host/port → app starts but **silently receives nothing**; new users never get profiles. Wrong topic name → same. Watch the logs for `Discovered coordinator` / partition assignment to confirm it connected. |
| **MySQL** (`userservice` schema) | `spring.datasource.*` | Wrong host/creds → app **fails to start** (or Hikari timeout in logs). Every read/write is dead. |
| **authservice** | *Indirectly, through Kafka only.* No direct HTTP call. | If authservice's producer topic name ≠ this consumer's topic name, or they point at different brokers, the two never meet. This is the #1 thing to get right. |
| **Kong / API gateway** (`authservice/services.yml`) | Routes external traffic to `:9810` | Only relevant when running the full compose stack. |

---

## Wired to YOUR machine (not the tutorial's)

The tutorial author's hosts are baked into config as **defaults**. Yours differ:

| Where | Tutorial default | Your reality | Fix |
|---|---|---|---|
| `application.properties` line 1 — Kafka host | `localhost` | authservice publishes to **`192.168.122.132:9092`** (your KVM/libvirt VM). userservice must read the *same* broker. | Run with env var `KAFKA_HOST=192.168.122.132` |
| `application.properties` — MySQL host | `mysql` (compose service name) | Your MySQL is on the VM at `192.168.122.132:3306` (or localhost if you run it on the host) | `MYSQL_HOST=192.168.122.132` |
| `application.properties` — MySQL user/pass | `root` / `password` | Whatever you set when you installed MySQL | `MYSQL_USER=... MYSQL_PASSWORD=...` |
| `authservice/services.yml` → kafka `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://127.0.0.1:9092` | tutorial's | If you run Kafka from that compose file **on the VM** and connect **from your host**, `127.0.0.1` is wrong — clients get told to reconnect to 127.0.0.1 and fail. | Set it to `PLAINTEXT://192.168.122.132:9092` |

> **No AWS Lightsail / EC2 IP is hardcoded anywhere.** Deployment (`authservice/.github/workflows/deploy.yml` + `cloudformation-template.yaml`) uses ECR + CloudFormation and reads everything from GitHub Actions secrets. `mysql.myapp.local` / `kafka.myapp.local` in the CloudFormation template are internal DNS names that only exist inside that AWS VPC — ignore them for local dev.

**Run locally, roughly:**
```bash
export KAFKA_HOST=192.168.122.132
export MYSQL_HOST=192.168.122.132
export MYSQL_USER=varun
export MYSQL_PASSWORD=varunkewlani
./gradlew bootRun
```

---

## YOUR TASKS — fill in the commented-out code

The project **will not compile** until TASK 2, 3, 4 are done (they reference each other).
Do them in order. Each spot is marked `// ===== TASK N =====` with a `// TODO(TASK N)` line.

| # | File | What to write | Concept you're practicing |
|---|---|---|---|
| **1** | `consumer/AuthServiceConsumer.java` | Add `@KafkaListener(topics = "${spring.kafka.topic-json.name}", groupId = "${spring.kafka.consumer.group-id}")` above `listen(...)`, and inside the `try` call `userService.createOrUpdateUser(eventData);` | How a service *receives* an event. Property placeholders in annotations. |
| **2** | `repository/UserRepository.java` | Declare `Optional<UserInfo> findByUserId(String userId);` | Spring Data derived queries — method name → SQL. |
| **3** | `entities/UserInfoDto.java` → `transformToUserInfo()` | `return UserInfo.builder().userId(userId).firstName(firstName).lastName(lastName).email(email).phoneNumber(phoneNumber).profilePic(profilePic).build();` | DTO ↔ Entity mapping; Lombok `@Builder`. |
| **4** | `service/UserService.java` → `createOrUpdateUser(...)` | `findByUserId(...)`, then `.map(u -> userRepository.save(dto.transformToUserInfo()))` `.orElseGet(() -> userRepository.save(dto.transformToUserInfo()))`, then build & return a `UserInfoDto` (copy the `new UserInfoDto(...)` call from `getUser()` below). | Upsert logic; `Optional` map/orElseGet; the service layer's job. |
| **5** | `consumer/UserController.java` → `createUpdateUser(...)` | Mirror `getUser()` above: `try` → call service, return `ResponseEntity<>(user, HttpStatus.OK)`; `catch` → `ResponseEntity<>(HttpStatus.NOT_FOUND)`. | Thin controllers; reusing the service layer from HTTP. |

### How to verify when done
1. `./gradlew build` compiles.
2. Start MySQL + Kafka (VM), then `./gradlew bootRun` — logs show Kafka partition assignment, no DB errors.
3. Start authservice, `POST /auth/v1/signup` with a body that has `first_name`, `last_name`, `email`, `phone_number`.
4. Check the `users` table in the `userservice` DB — a row should appear (that's TASK 1→4 working).
5. `POST http://localhost:9810/user/v1/createUpdate` and `GET /user/v1/getUser` with a `UserInfoDto` JSON body — should round-trip (that's TASK 5).
