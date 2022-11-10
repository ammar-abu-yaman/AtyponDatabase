package com.atypon.project.worker.auth;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public class User implements Serializable {

    public enum Role {Admin, Standard}

    private String username;
    private String passwordHash;
    private Role role;
    private String nodeId;


    public User(String username, String passwordHash, Role role, String nodeId) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.role = role;
        this.nodeId = nodeId;
    }

    public static Role getRole(String role) {
        switch (role.toLowerCase()) {
            case "admin":
                return Role.Admin;
            case "standard":
                return Role.Standard;
        }
        return null;
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return "User{" +
                "username='" + username + '\'' +
                ", passwordHash='" + passwordHash + '\'' +
                ", role=" + role +
                ", nodeId='" + nodeId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(username, user.username) && Objects.equals(passwordHash, user.passwordHash) && role == user.role && Objects.equals(nodeId, user.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, passwordHash, role, nodeId);
    }
}
