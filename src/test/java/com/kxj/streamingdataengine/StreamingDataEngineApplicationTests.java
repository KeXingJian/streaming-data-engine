package com.kxj.streamingdataengine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@ExtendWith(com.kxj.streamingdataengine.extension.TestReportExtension.class)
class StreamingDataEngineApplicationTests {

    @Test
    void contextLoads() {
    }

}
