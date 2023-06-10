package com.pipeline.api;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@CrossOrigin(origins="*", allowedHeaders = "*")
@RequiredArgsConstructor
@RestController
public class ProduceController {
	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
	private static final Gson gson = new GsonBuilder().create();
	private final KafkaTemplate<String, String> kafkaTemplate;

	@GetMapping("/api/select")
	public void selectColor(
		@RequestHeader("user-agent") String userAgentName,
		@RequestParam("color") String colorName,
		@RequestParam("user") String userName) {
		UserEventVO userEventVO = new UserEventVO(SIMPLE_DATE_FORMAT.format(new Date()), userAgentName, colorName, userName);
		String jsonColorLog = gson.toJson(userEventVO);

		kafkaTemplate.send("select-color", jsonColorLog)
			.handle((result, ex) -> {
				if (ex != null) {
					log.error("[Producer] 브로커로 데이터 전송 중 오류 발생", ex);
					return null;
				}
				log.info("[Producer] 브로커로 데이터 전송 성공. 전송 데이터: {}", result.toString());
				return result;
			});
	}
}

