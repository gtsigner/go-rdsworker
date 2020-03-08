package rdsworker

import (
    "zhaojunlike/akamai/web"
    "zhaojunlike/common"
    "zhaojunlike/common/chttp"
)

type AbCkCookie struct {
    ExpireTime int64             `json:"expire_time,omitempty"`
    CreateTime int64             `json:"create_time,omitempty"` //创建时间
    Proxy      *chttp.Proxy      `json:"proxy,omitempty"`
    UseTime    int               `json:"use_time,omitempty"`
    AkaCookie  *web.AkaCookieMap `json:"aka_cookie,omitempty"`
    BmakConfig *web.DyBmakConfig `json:"bmak_config,omitempty"` //aka配置
}

func NewAbCkCookie(cookie *web.AkaCookieMap, bmak *web.DyBmakConfig) AbCkCookie {
    var ck = AbCkCookie{
        CreateTime: common.CreateTimestamp(),
        ExpireTime: 0,
        AkaCookie:  cookie,
        BmakConfig: bmak,
    }
    return ck
}
