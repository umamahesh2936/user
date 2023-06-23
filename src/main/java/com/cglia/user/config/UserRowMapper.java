package com.cglia.user.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.cglia.user.dto.User;

/**
 * this class is for giving the row mapper object into the while fetching the
 * data from db to the particular csv file
 */
public class UserRowMapper implements RowMapper<User> {

	@Override
	public User mapRow(ResultSet rs, int rowNum) throws SQLException {
		User user = new User();
		user.setId(rs.getInt("id"));
		user.setName(rs.getString("name"));
		user.setPhone(rs.getLong("phone"));
		user.setStatus(rs.getInt("status"));

		return user;
	}

}
