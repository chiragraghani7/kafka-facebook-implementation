package com.javatechie.social.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.social.connect.ConnectionFactoryLocator;
import org.springframework.social.connect.support.ConnectionFactoryRegistry;
import org.springframework.social.facebook.connect.FacebookConnectionFactory;

@Configuration
public class ConnectionFactoryConfig {

    @Bean
    public ConnectionFactoryLocator connectionFactoryLocator() {
        ConnectionFactoryRegistry registry = new ConnectionFactoryRegistry();
        registry.addConnectionFactory(new FacebookConnectionFactory(facebookClientId, facebookClientSecret));
        return registry;
    }
    @Value("${facebook.clientId}")
    private String facebookClientId;

    @Value("${facebook.clientSecret}")
    private String facebookClientSecret;

}
