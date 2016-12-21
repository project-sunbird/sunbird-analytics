package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestConsoleDispatcher extends BaseSpec {

    "ConsoleDispatcher" should "send output to console" in {
        noException should be thrownBy {
            ConsoleDispatcher.dispatch(Array("test"), Map("" -> ""));
        }
    }

}