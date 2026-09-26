package com.singhdevhub.userservice.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.singhdevhub.userservice.entities.UserInfoDto;
import com.singhdevhub.userservice.repository.UserRepository;
import com.singhdevhub.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AuthServiceConsumer
{

    @Autowired
    private UserService userService;

    @Autowired
    private ObjectMapper objectMapper;

    // ===== TASK 1: the Kafka listener =====
    // This is how userservice RECEIVES the "new user" message that authservice published.
    // Spring calls listen(...) automatically for every message that lands on the topic.
    // `eventData` is already a UserInfoDto because UserInfoDeserializer turned the raw
    // JSON bytes into an object before this method runs.
    //
    // What to write:
    //   a) Put this annotation directly above the method:
    //        @KafkaListener(topics = "${spring.kafka.topic-json.name}", groupId = "${spring.kafka.consumer.group-id}")
    //      - topics  -> which Kafka topic to read; the ${...} pulls "user_service" from application.properties
    //      - groupId -> the consumer group name; pulls "userinfo-consumer-group" from application.properties
    //   b) Inside the try block, hand the event to the service layer:
    //        userService.createOrUpdateUser(eventData);
    @KafkaListener(topics = "${spring.kafka.topic-json.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(UserInfoDto eventData) {
        try{
            userService.createOrUpdateUser(eventData);
            // Todo: Make it transactional, to handle idempotency and validate email, phoneNumber etc
            // TODO(TASK 1): call userService.createOrUpdateUser(eventData);
        }catch(Exception ex){
            ex.printStackTrace();
            System.out.println("AuthServiceConsumer: Exception is thrown while consuming kafka event");
        }
    }

}
