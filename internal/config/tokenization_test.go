package config

import (
 "testing"
 "time"
 "github.com/rislanov/vllm-priority-gateway/internal/coordination"
)
func TestTokenizationConfig(t *testing.T){
 c:=defaultConfig();if c.Tokenization!=coordination.DefaultTokenizationPolicy(){t.Fatal(c.Tokenization)}
 for _,v:=range []struct{key,value string}{{"LLMGW_TOKENIZE_CLIENT_MAX_INFLIGHT","0"},{"LLMGW_TOKENIZE_BACKEND_MAX_INFLIGHT","10001"},{"LLMGW_TOKENIZE_CLIENT_RPM","10000001"},{"LLMGW_TOKENIZE_TIMEOUT","301s"},{"LLMGW_TOKENIZE_TIMEOUT","invalid"}}{
  t.Run(v.key+v.value,func(t *testing.T){c:=defaultConfig();if e:=c.loadTokenization(func(k string)(string,bool){return v.value,k==v.key});e==nil{t.Fatal("invalid setting accepted")}})
 }
 if e:=c.loadTokenization(func(k string)(string,bool){return "1s",k=="LLMGW_TOKENIZE_TIMEOUT"});e!=nil || c.Tokenization.Timeout!=time.Second{t.Fatal(c.Tokenization,e)}
}
