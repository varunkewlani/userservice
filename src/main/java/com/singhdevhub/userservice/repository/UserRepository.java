package com.singhdevhub.userservice.repository;

import com.singhdevhub.userservice.entities.UserInfo;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@EnableJpaRepositories
public interface UserRepository extends CrudRepository<UserInfo, String>
{

    // ===== TASK 2: a Spring Data "derived query" =====
    // Spring Data JPA writes the SQL for you IF the method name follows a pattern:
    //   findBy<FieldName>  ->  SELECT * FROM users WHERE <that column> = ?
    // The field is "userId" on the UserInfo entity, so the method is findByUserId.
    // Returning Optional<UserInfo> means "maybe a row, maybe nothing" (avoids null checks).
    //
    // What to write (just the signature, no body - this is an interface):
    //   Optional<UserInfo> findByUserId(String userId);
    //
    // NOTE: UserService.getUser() and TASK 4 both call this, so nothing compiles until it exists.
    // TODO(TASK 2): declare findByUserId here
    Optional<UserInfo> findByUserId(String userIdl);

}
