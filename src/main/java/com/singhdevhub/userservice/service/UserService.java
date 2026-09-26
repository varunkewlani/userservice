package com.singhdevhub.userservice.service;

import com.singhdevhub.userservice.entities.UserInfo;
import com.singhdevhub.userservice.entities.UserInfoDto;
import com.singhdevhub.userservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

@Service
@RequiredArgsConstructor
public class UserService
{
    @Autowired
    private final UserRepository userRepository;

    // ===== TASK 4: "upsert" = update the row if it exists, otherwise insert a new one =====
    // This runs for every Kafka event (via TASK 1) and for the POST endpoint (TASK 5).
    // Steps:
    //   1. Look the user up:   userRepository.findByUserId(userInfoDto.getUserId())  -> Optional<UserInfo>
    //   2. Whether found or not, the action is the same here: save the incoming values.
    //        userRepository.save(userInfoDto.transformToUserInfo());
    //      - Optional.map(x -> ...)      runs when a row was found  (update path)
    //      - Optional.orElseGet(() -> ...) runs when nothing found  (insert path)
    //   3. Copy the saved UserInfo back into a UserInfoDto and return it
    //      (see getUser() below for the exact "new UserInfoDto(...)" call - copy that).
    //
    // Tip: the getUser() method right below is a smaller worked example of the same
    //      "find -> unwrap -> copy to DTO" shape. Read it first.
    public UserInfoDto createOrUpdateUser(UserInfoDto userInfoDto){
            Optional<UserInfo> existing = userRepository.findByUserId(userInfoDto.getUserId());
            UserInfo saved = existing
                .map(u  -> userRepository.save(userInfoDto.transformToUserInfo()))
                .orElseGet(() -> userRepository.save(userInfoDto.transformToUserInfo()));
            
            return new UserInfoDto(
                saved.getUserId(),
                saved.getFirstName(),
                saved.getLastName(),
                saved.getPhoneNumber(),
                saved.getEmail(),
                saved.getProfilePic()
        );

        // TODO(TASK 4): find-by-id, then save (update or insert), then return the saved user as a UserInfoDto
        // return null;
    }

    public UserInfoDto getUser(UserInfoDto userInfoDto) throws Exception{
        Optional<UserInfo> userInfoDtoOpt = userRepository.findByUserId(userInfoDto.getUserId());
        if(userInfoDtoOpt.isEmpty()){
            throw new Exception("User not found");
        }
        UserInfo userInfo = userInfoDtoOpt.get();
        return new UserInfoDto(
                userInfo.getUserId(),
                userInfo.getFirstName(),
                userInfo.getLastName(),
                userInfo.getPhoneNumber(),
                userInfo.getEmail(),
                userInfo.getProfilePic()
        );
    }

}
