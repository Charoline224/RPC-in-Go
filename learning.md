

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


为什么返回指针：
sync.Mutex 不能被复制——如果返回值类型，Go 会复制整个 Client 结构体，锁也被复制了，就失效了。这也是你那个警告的原因！


Q:为什么call的channel的容量为1？
A:想想这个场景：
如果 Done 是无缓冲的 make(chan *Call)，Receive 往里发信号 call.Done <- call 的时候，如果用户还没有在读这个 channel，Receive 会阻塞在那里。
但 Receive 是一个循环，它阻塞了就没法继续处理下一个响应了！
缓冲为 1，Receive 发完信号就走，不需要等用户来取。

每次写 channel 的时候，问自己两个问题：

发送方：发完信号之后需要等待吗？还是发完就走？
接收方：什么时候来取？会不会比发送方慢？



Server 注册方法的时候，已经通过反射知道了参数类型（ArgType）
客户端发来字节流
Server 用 reflect.New(ArgType) 创建一个空的该类型实例
把字节流解码填充进这个实例

所以不是从字节流猜类型，而是已经知道类型，用类型来解码字节流。



method.Func.Call([]reflect.Value{receiver, args, reply})
receiver 是服务实例（MathService 的实例）
args 是参数
reply 是返回值指针



反射得到的结果是一个容器，不能直接作为interface参数传入。
如果你直接传 res（reflect.Value），gob.Encoder 收到的是一个 reflect.Value 对象，它会把 reflect.Value 这个结构体本身编码，而不是里面包装的实际值！
res.Interface() 把 reflect.Value 里包装的实际值取出来，这样编码的才是真正的数据。