package com.singhdevhub.userservice.consumer;

import com.singhdevhub.userservice.entities.UserInfoDto;
import com.singhdevhub.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class UserController
{

    @Autowired
    private UserService userService;

    @GetMapping("/user/v1/getUser")
    public ResponseEntity<UserInfoDto> getUser(@RequestBody UserInfoDto userInfoDto){
        try{
            UserInfoDto user = userService.getUser(userInfoDto);
            return new ResponseEntity<>(user, HttpStatus.OK);
        }catch (Exception ex){
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    // ===== TASK 5: an HTTP endpoint that reuses the service layer =====
    // @PostMapping("/user/v1/createUpdate") already maps POST requests to this method.
    // A controller method's job is tiny: call the service, wrap the result in a ResponseEntity
    // (HTTP status + body). Copy the shape of getUser() right above:
    //   try   -> UserInfoDto user = userService.createOrUpdateUser(userInfoDto);
    //            return new ResponseEntity<>(user, HttpStatus.OK);
    //   catch -> return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    @PostMapping("/user/v1/createUpdate")
    public ResponseEntity<UserInfoDto> createUpdateUser(UserInfoDto userInfoDto){
        // TODO(TASK 5): implement this handler like getUser()
        // return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
        try{
            UserInfoDto user = userService.createOrUpdateUser(userInfoDto);
            return new ResponseEntity<>(user, HttpStatus.OK);
        }
        catch(Exception e){
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Boolean> checkHealth(){
        return new ResponseEntity<>(true, HttpStatus.OK);
    }

}
