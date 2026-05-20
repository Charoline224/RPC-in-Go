

Option 层：暗号（MagicNumber）+ 协议协商（CodecType）→ 只发一次
消息层：Header + Body → 重复发很多次
### codec作用：
struct <-> 字节流
写发到网络连接里面，就要用字节流-encode

### 对于client的结构构建，从以下几个方面考虑：

client作用是：
“作为发出调用远程函数的申请并且收到调用函数值的对象”

1. 通信

知道往哪发 → conn
知道怎么编码 → codec

2. 并发请求处理

给请求一个唯一编号 → cnt
知道这个响应对应哪个请求 → calls map
通知等待的 goroutine → 每个 Call 里的 Done channel
保护 calls map → 锁

3. 自身状态

有没有err
有没有关闭客户端