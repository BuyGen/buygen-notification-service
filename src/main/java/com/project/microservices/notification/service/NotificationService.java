package com.project.microservices.notification.service;

import com.project.microservices.order.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service


public class NotificationService {

    private final JavaMailSender javaMailSender;
    public NotificationService(JavaMailSender javaMailSender) {
        this.javaMailSender = javaMailSender;
    }
    private static final Logger log =
            LoggerFactory.getLogger(NotificationService.class);
    @KafkaListener(topics = "order-placed", groupId = "notification-service")
    public void listen(GenericRecord record) {
        log.info("🔥 Kafka listener triggered");
        log.info("Received raw Avro record: {}", record);

        String email = record.get("email").toString();
        String firstName = Objects.toString(record.get("firstName"), "");
        String lastName = Objects.toString(record.get("lastName"), "");
        String orderNumber = record.get("orderNumber").toString();

        MimeMessagePreparator messagePreparator = mimeMessage -> {
            MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage);
            messageHelper.setFrom("springshop@email.com");
            messageHelper.setTo(email);
            messageHelper.setSubject(
                    "Your Order with OrderNumber %s is placed successfully"
                            .formatted(orderNumber)
            );
            messageHelper.setText("""
                Hi %s %s,

                Your order with order number %s is now placed successfully.

                Best Regards
                - BuyGen The New Gen Shop
                """.formatted(firstName, lastName, orderNumber));
        };

        try {
            javaMailSender.send(messagePreparator);
            log.info("Order Notification email sent!!");
        } catch (MailException e) {
            log.error("Exception occurred when sending mail", e);
            throw new RuntimeException(e);
        }
    }



    }
