package com.singhdevhub.userservice.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true) // ignores for those fields which don't have any value
public class UserInfoDto
{

    @JsonProperty("user_id")
    @NonNull
    private String userId;

    @JsonProperty("first_name")
    @NonNull
    private String firstName;

    @JsonProperty("last_name")
    @NonNull
    private String lastName;

    @JsonProperty("phone_number")
    @NonNull
    private Long phoneNumber;

    @JsonProperty("email")
    @NonNull
    private String email;

    @JsonProperty("profile_pic")
    private String profilePic;

    // ===== TASK 3: map this DTO to the database entity =====
    // UserInfoDto = the shape of data arriving from Kafka / an HTTP body (a "transfer" object).
    // UserInfo    = the shape stored in the DB (the @Entity class).
    // They carry almost the same fields; this method copies this DTO's values into a new
    // UserInfo so the repository can save it. UserInfo has Lombok's @Builder, so the pattern is:
    //   UserInfo.builder().userId(userId).firstName(firstName). ... .build();
    // Fields to copy: userId, firstName, lastName, email, phoneNumber, profilePic.
    public UserInfo transformToUserInfo() {

        //very important and oimple to know:
        //you could directly use modelmapper instead of this method like:
        
        //what it does: Java object → another Java object

        //ModelMapper modelMapper = new ModelMapper();
        //UserInfo entity = modelMapper.map(userInfoDto, UserInfo.class);


        return UserInfo.builder().userId(userId).firstName(firstName).lastName(lastName).email(email).phoneNumber(phoneNumber).profilePic(profilePic).build();
        // TODO(TASK 3): build and return a UserInfo from this DTO's fields
        // return null;
    }

}
