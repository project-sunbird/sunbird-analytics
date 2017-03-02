package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.BaseSpec

class TestScriptDispatcher extends BaseSpec {
    
    "ScriptDispatcher" should "execute the script" in {
        
        val events = ScriptDispatcher.dispatch("ls");
        
        a[Exception] should be thrownBy {
            ScriptDispatcher.dispatch(null);
        }
    }
  
}