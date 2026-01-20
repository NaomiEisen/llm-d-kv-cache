# KV-cache abstrction! :D
---
## Questions
- **KVEventPublisher:** Are vllm and SGlang provide configurable "eventPblisher"? How to use it?
    
    *Maybe we should just use this for the abstraction- NO! We still should abstract, this would be a nice road for implementing different publishers in the future*

    **Answer:** 
    - Vllm: I found it in the code ([register_publisher](https://github.com/vllm-project/vllm/blob/71832ba71e77c01a1d249e014384522b46813749/vllm/distributed/kv_events.py#L480)), but I couldn't find it in any documentation. Maybe it will be a configurable feature in the future (or just other publisher options).

    [link to code snippet](https://github.com/vllm-project/vllm/blob/71832ba71e77c01a1d249e014384522b46813749/examples/online_serving/disaggregated_serving/kv_events.sh#L44)
    ```
     '{"enable_kv_cache_events": true, "publisher": "zmq", "topic": "kv-events"}'
    ```
    - SGlang: Same.
    [link to code snippet](https://github.com/sgl-project/sglang/blob/8916b9d080acec83c23f1d3c8f5d74d1738ebc8f/test/manual/test_kv_events.py#L44)
    ```
    '{"publisher": "zmq", "topic": "kv-events"}',
   ```

- **Hash** Validate if the Hash is calculated the same for both SGlang and vllm.

    Until I do so, we can just assume that they are not (which is 99% the case), and would not mix different engines in the same pool/index.

---
## Current state

### Code View
```
pkg/kvevents/
├── pool.go                    # Pool with vLLM-specific processing
│   └── Pool struct
│
├── events.go                  # vLLM event definitions
│   ├── EventBatch struct      # msgpack array format
│   └── Event struct           
│       ├── BlockStored            # vLLM event struct
│       ├── BlockRemoved           # vLLM event struct
│       └── AllBlocksCleared       # vLLM event struct
│
├── zmq_subscriber.go          # ZMQ-specific subscriber
│   └── zmqSubscriber struct
│
└── subscriber_manager.go      # Manages zmqSubscribers
    └── SubscriberManager
```

### Object diagram
```mermaid
    classDiagram
        class zmqSubscriber {
            -pool Pool
            -endpoint string
            -remote bool
            -topicFilter string
            +Start(ctx)
            -runSubscriber(ctx)
        }
        
        class SubscriberManager {
            -pool Pool
            -subscribers map[string]subscriberEntry
            +EnsureSubscriber(podID, endpoint, topic, remote)
            +RemoveSubscriber(podID)
            +Shutdown()
        }
        
        class subscriberEntry {
            -subscriber zmqSubscriber
            -cancel CancelFunc
            -endpoint string
        }
        
        class Message {
            +Topic string
            +Payload []byte
            +Seq uint64
            +PodIdentifier string
            +ModelName string
        }
        
        class Pool {
            -queues []WorkQueue
            -concurrency int
            -index Index
            -tokenProcessor TokenProcessor
            +Start(ctx)
            +AddTask(msg)
            +Shutdown(ctx)
            -worker(ctx, workerIndex)
            -processEvent(ctx, msg)
            -digestEvents(ctx, podID, model, events)
        }
        
        class EventBatch {
            +TS float64
            +Events []RawMessage
            +DataParallelRank int
        }
        
        class event {
            <<interface>>
            +isEvent()
            +ToTaggedUnion() []any
        }
        
        class BlockStored {
            +BlockHashes []any
            +ParentBlockHash any
            +TokenIds []uint32
            +BlockSize int
            +LoraID int
            +Medium string
            +LoraName string
            +isEvent()
            +ToTaggedUnion() []any
        }
        
        class BlockRemoved {
            +BlockHashes []any
            +Medium string
            +isEvent()
            +ToTaggedUnion() []any
        }
        
        class AllBlocksCleared {
            +isEvent()
            +ToTaggedUnion() []any
        }
        
        class Index {
            <<interface>>
            +Add(engineKeys, requestKeys, pods)
            +Evict(engineKey, pods)
            +GetRequestKey(engineKey) Key
        }
        
        class TokenProcessor {
            <<interface>>
            +TokensToKVBlockKeys(parent, tokens, model) []Key
        }
        
        %% Relationships
        SubscriberManager --> subscriberEntry : manages
        subscriberEntry --> zmqSubscriber : contains
        
        zmqSubscriber --> Pool : sends Message to
        zmqSubscriber --> Message : creates
        
        Pool --> Message : processes
        Pool --> Index : updates
        Pool --> TokenProcessor : uses
        
        Message --> EventBatch : contains (encoded)
        EventBatch --> event : contains (RawMessage)
        
        event <|.. BlockStored : Implements
        event <|.. BlockRemoved : Implements
        event <|.. AllBlocksCleared : Implements
        
        Pool ..> BlockStored : unmarshals & processes
        Pool ..> BlockRemoved : unmarshals & processes
        Pool ..> AllBlocksCleared : unmarshals & processes
        
        note for zmqSubscriber "vLLM-specific:<br/>- ZMQ transport hardcoded<br/>- msgpack assumed<br/>- Topic parsing hardcoded"
        note for Pool "vLLM-specific:<br/>- msgpack.Unmarshal()<br/>- getHashAsUint64()<br/>- Switch statement on event types"
        note for event "vLLM event format:<br/>Tagged union array<br/>['EventType', ...fields]"
```

### Data Flow
```mermaid
sequenceDiagram
    participant vLLM as vLLM Engine
    participant ZMQ as ZMQ Socket
    participant Subscriber as zmqSubscriber
    participant Pool as Pool
    participant Index as Index
    
    vLLM->>ZMQ: Publish msgpack event
    Note over vLLM: Topic: kv@pod-id@model
    
    ZMQ->>Subscriber: RecvMessageBytes()
    Note over ZMQ: 3-part message:<br/>[topic, seq, payload]
    
    Subscriber->>Subscriber: Extract pod/model from topic
    Note over Subscriber: Parse "kv@pod-id@model"
    
    Subscriber->>Pool: AddTask(Message)
    Note over Subscriber: Message contains:<br/>- Raw msgpack payload<br/>- PodIdentifier<br/>- ModelName
    
    activate Pool
    Pool->>Pool: Hash PodIdentifier → Select queue
    
    Pool->>Pool: Unmarshal msgpack → EventBatch
    
    Pool->>Pool: UnmarshalKVEvent() for each event
    
    Pool->>Pool: digestEvents()
    Note over Pool: Switch on event type
    
    alt BlockStored
        Pool->>Pool: getHashAsUint64(BlockHashes)
        Note over Pool: Convert uint64 or []byte
        Pool->>Pool: TokensToKVBlockKeys()
        Pool->>Index: Add(engineKeys, requestKeys, pods)
    else BlockRemoved
        Pool->>Pool: getHashAsUint64(BlockHashes)
        Pool->>Index: Evict(engineKey, pods)
    else AllBlocksCleared
        Note over Pool: Currently ignored
    end
    
    deactivate Pool
```

### Problmes:
- The use of ZMQ and msgpack is tightly integrated within Events and Subscriber structures.
- The pool handles "events" logic. This logic should be separated from the pool, Events should be able to "digest" themselves.


---
## New Architecture - Engine Adapter + Transport + Decode Interfaces

The goal is to seperate the processing logic into a deticated units (objects/structures):
- **Transport:** The dependency on a specific transport protocol (ZMQ, HTTP, etc.) will be encapsulated within 'Transport' interface.
- **Decode:** The dependency on a specific event serilazation (msgpack, JSON, etc.) will be encapsulated within 'Decoder' interface.
- **Message Struct:** The dependency on a specific event struct based on LLM engine (vLLM, SGLang, etc.) will be encapsulated within 'EngineAdapter' interface.

### Simplified Overvirew
A simplified overview of the new components, just to get the feeling (This is just an example, I know that SGLang does use HTTP and JSON).
The relationship is:

```
Subscriber {
    EngineAdapter {
       Transport
        Decoder 
    }
}
```

```mermaid
graph TB
    SM[SubscriberManager]
    
    SM --> S1[Subscriber 1<br/>pod: vllm-pod-1]
    SM --> S2[Subscriber 2<br/>pod: sglang-pod-1]
    
    S1 --> A1[VLLMAdapter]
    A1 --> T1[ZMQTransport]
    A1 --> D1[MsgpackDecoder]
    
    S2 --> A2[SGLangAdapter]
    A2 --> T2[HTTPTransport]
    A2 --> D2[JSONDecoder]
    
    SM --> Pool
    S1 --> Pool
    S2 --> Pool

    
    style SM fill:#b3e0ff,color:#000
    style S1 fill:#ffe4b3,color:#000
    style S2 fill:#ffe4b3,color:#000
    style A1 fill:#c8f0c8,color:#000
    style A2 fill:#c8f0c8,color:#000
```

***Note: Theoretically this scenario is possible but for now it is better to separate different engines with different pools (so, SubscriberManager). This is just to emphasize the flexibility with this design***

For now, it will look something like this:
```mermaid
graph TB
    SM[SubscriberManager-vllm]
    SM2[SubscriberManager-sglang]
    
    SM --> S1[Subscriber 1<br/>pod: vllm-pod-1]
    SM2 --> S2[Subscriber 2<br/>pod: sglang-pod-1]
    
    S1 --> A1[VLLMAdapter]
    A1 --> T1[ZMQTransport]
    A1 --> D1[MsgpackDecoder]
    
    S2 --> A2[SGLangAdapter]
    A2 --> T1[ZMQTransport]
    A2 --> D1[MsgpackDecoder]
    
    SM --> Pool-vllm
    SM2 --> Pool-sglang
    S1 --> Pool-vllm
    S2 --> Pool-sglang

    
    style SM fill:#b3e0ff,color:#000
    style SM2 fill:#b3e0ff,color:#000
    style S1 fill:#ffe4b3,color:#000
    style S2 fill:#ffe4b3,color:#000
    style A1 fill:#c8f0c8,color:#000
    style A2 fill:#c8f0c8,color:#000
```

### Object Diagram

A full object diagram of the design.

```mermaid
classDiagram
    class Transport {
        <<interface>>
        +Connect(endpoint) error
        +Receive() bytes
        +Close() error
    }
    %% Implementation of Transport interface
    class ZMQTransport {
        -socket ZMQ
        +Connect()
        +Receive()
    }
    
    class Decoder {
        <<interface>>
        +Decode(bytes, v) error
    }
    
    %% Implementation of Transport interface
    class MsgpackDecoder {
        +Decode() error
    }
    
    class EngineAdapter {
        <<interface>>
        -transport Transport
        -decoder Decoder
        +Name() string
        +GetTransport() Transport
        +ProcessMessage(bytes) GenericEventBatch
        +ParseHash()
    }
    
    class VLLMAdapter {
        -message Message %% The format we excpect to recieve
        -transport Transport
        -decoder Decoder
        +ProcessMessage() GenericEventBatch
        +ParseHash()
        -parseVLLMEvents()
    }
    
    class SGLangAdapter {
        -transport Transport
        -decoder Decoder
        +ProcessMessage() GenericEventBatch
        +ParseHash()
        -parseSGLangEvents()
    }
    
    class GenericEvent {
        <<interface>>
        +Type() EventType
        +Process(ctx, index, tokenProcessor, podID, model, adapter) error
    }
    
    class BlockStoredEvent {
        -BlockHashes []any
        -Tokens []uint32
        -ParentHash any
        -DeviceTier string
        +Type() EventType
        +Process() error
    }
    
    class BlockRemovedEvent {
        -BlockHashes []any
        +Type() EventType
        +Process() error
    }
    
    class AllBlocksClearedEvent {
        +Type() EventType
        +Process() error
    }
    
    class GenericEventBatch {
        +Timestamp float64
        +Events []GenericEvent
    }
    
    class Subscriber {
        -adapter EngineAdapter
        -pool Pool
        -endpoint string
        +Start(ctx)
        +Stop()
    }
    
    class Pool {
        -queues []WorkQueue
        -index Index
        -tokenProcessor TokenProcessor
        +AddTask(msg)
        -processEvent(msg)
    }
    
    class Index {
        <<interface>>
        +Add(keys, requestKeys, pods)
        +Evict(keys, pods)
        +ClearPod(podID)
    }
    
    %% Relationships
    Transport <|.. ZMQTransport
    
    Decoder <|.. MsgpackDecoder
    
    EngineAdapter <|.. VLLMAdapter
    EngineAdapter <|.. SGLangAdapter
    
    VLLMAdapter --> Transport : contains
    VLLMAdapter --> Decoder : contains
    SGLangAdapter --> Transport : contains
    SGLangAdapter --> Decoder : contains
    
    GenericEvent <|.. BlockStoredEvent
    GenericEvent <|.. BlockRemovedEvent
    GenericEvent <|.. AllBlocksClearedEvent
    
    GenericEventBatch --> GenericEvent : contains
    
    Subscriber --> EngineAdapter : uses
    Subscriber --> Pool : AddTask(Message)
    
    VLLMAdapter ..> GenericEvent : creates
    
    SGLangAdapter ..> GenericEvent : creates
    
    Pool --> GenericEventBatch : processes
    
    BlockStoredEvent --> Index : updates
    BlockRemovedEvent --> Index : updates
    
    GenericEvent --> EngineAdapter : uses ParseHash()
```

### Data Flow

```mermaid
sequenceDiagram
    participant Engine as LLM Engine<br/>(vLLM/SGLang)
    participant Transport as Transport<br/>(ZMQ/HTTP/...)
    participant Decoder as Decoder<br/>(msgpack/JSON/...)
    participant Subscriber as Subscriber
    participant Adapter as EngineAdapter
    participant Event as GenericEvent
    participant Pool as Pool
    participant Index as Index
    
    Engine->>Transport: Publish event
    Note over Engine: Engine-specific format
    
    Transport->>Subscriber: Receive() → rawBytes
    Note over Transport: Protocol layer<br/>returns raw bytes
    
    Subscriber->>Adapter: ProcessMessage(rawBytes)
    activate Adapter
    
    Adapter->>Decoder: Decode(rawBytes)
    activate Decoder
    Note over Decoder: Unmarshal bytes<br/>to Message
    Decoder-->>Adapter: Decoded Message
    deactivate Decoder
    
    Note over Adapter: Parse Mesage<br/>Create generic events
    Adapter->>Event: new BlockStoredEvent()
    Adapter-->>Subscriber: GenericEventBatch{Events}
    deactivate Adapter
    
    Subscriber->>Pool: AddTask(GenericEventBatch)
    Note over Subscriber: Processed Batch - array of GenericEvent
    
    activate Pool
    Pool->>Event: event.Process(ctx, index, ...)
    activate Event
    Note over Event: Self-processing:<br/>1. Parse hashes<br/>2. Create keys<br/>3. Update index
    Event->>Adapter: ParseHash(rawHash)
    Adapter-->>Event: for example- the uint64
    Event->>Index: Add/Evict/Clear
    deactivate Event
    deactivate Pool
```

### Architecture Layers Summary

```
┌─────────────────────────────────────────────────────────┐
│    Transport                                            │
│    - ZMQ, HTTP, etc.                                    │
│    - Returns raw bytes                                  │
└─────────────────────────────────────────────────────────┘
                         ↓ raw bytes
┌─────────────────────────────────────────────────────────┐
│    Decoder                                              │
│    - msgpack, JSON, etc.                                │
│    - Unmarshals bytes to structs: Decode()              │
└─────────────────────────────────────────────────────────┘
                         ↓ decoded structs
┌─────────────────────────────────────────────────────────┐
│    Adapter (Translation to GenericEvent)                │
│    - Contains Transport + Decoder                       │
│    - Uses transport to recieve bytes.                   │
│    - Uses Decoder to creates generic events             │
└─────────────────────────────────────────────────────────┘
                         ↓ GenericEventBatch
┌─────────────────────────────────────────────────────────┐
│    Event                                                │
│    - BlockStoredEvent, BlockRemovedEvent, etc.          │
│    - Self-processing events: Digest()                   │
└─────────────────────────────────────────────────────────┘
                         ↓ event.Process()
┌─────────────────────────────────────────────────────────┐
│    Pool                                                 │
│    - Calls event.Digest() (no switch statement!)        │
│    - Manages worker queues                              │
└─────────────────────────────────────────────────────────┘
                         ↓ index 
┌─────────────────────────────────────────────────────────┐
│    Index                                                │
│    - Add/Evict/Clear                                    │
└─────────────────────────────────────────────────────────┘

From subsrciber side:
┌─────────────────────────────────────────────────────────────────────┐
│    SubscriberManager                                                │
│    - Creates and manages multiple Subscribers                       │
│    - Passes Pool reference to each Subscriber                       │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ creates
┌─────────────────────────────────────────────────────────────────────┐
│    Subscriber                                                       │
│    - Holds EngineAdapter                                            │
│    - Sends processed events to Pool                                 │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ uses
┌─────────────────────────────────────────────────────────────────────┐
│    EngineAdapter (interface)                                        │
│    - VLLMAdapter / SGLangAdapter                                    │
│    - Contains Transport + Decoder                                   │
│    - ProcessMessage(): bytes → GenericEventBatch                    │
│    - ParseHash(): engine format → uint64                            │
└─────────────────────────────────────────────────────────────────────┘
                    ↓ contains                ↓ contains
        ┌───────────────────────┐   ┌───────────────────────┐
        │     Transport Layer   │   │     Decoder Layer     │
        │ - ZMQ / HTTP          │   │ - msgpack / JSON      │
        │ - Connect/Bind        │   │ - Unmarshal bytes     │
        │ - Receive bytes       │   │                       │
        └───────────────────────┘   └───────────────────────┘
                              ↓ EngineAdapter returns GenericEventBatch
┌─────────────────────────────────────────────────────────────────────┐
│    GenericEvent (interface)                                         │
│    - BlockStoredEvent / BlockRemovedEvent / AllBlocksClearedEvent   │
│    - Self-processing: event.Process()                               │
│    - Each event knows how to update Index                           │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ event.Process()
┌─────────────────────────────────────────────────────────────────────┐
│    Pool                                                             │
│    - Receives GenericEventBatch from Subscribers                    │
│    - Calls event.Process() for each event                           │
│    - no switch statements!                                          │
└─────────────────────────────────────────────────────────────────────┘
                              ↓ index operations
┌─────────────────────────────────────────────────────────────────────┐
│    Index                                                            │
└─────────────────────────────────────────────────────────────────────┘

```
### Code View

```
pkg/kvevents/
├── transport/
│   ├── transport.go        # Transport interface
│   ├──── zmq.go              # ZMQ implementation
│   └──── http.go             # EXAMPLE: HTTP implementation
│
├── decoder/
│   ├── decoder.go          # Decoder interface
│   ├──── msgpack.go          # Msgpack implementation
│   └──── json.go             # EXAMPLE: JSON implementation
│
├── adapter/
│   ├── adapter.go          # EngineAdapter interface. contains: Transport, Decoder
│   ├──── vllm.go             # VLLMAdapter
│   └──── sglang.go           # EXAMPLE: SGLangAdapter
│
├── events.go                  
│   ├── Event                 # GenericEvent interface
│   └── EventBatch            # GenericEventBatch
│       ├── BlockStored            
│       ├── BlockRemoved           
│       └── AllBlocksCleared       
│
├── pool.go                 # Pool implementation
├── subscriber.go           # Subscriber implementation
└── subscriber_manager.go   # SubscriberManager
```
---

##  Notes
- Should verify that the "self processing events" is a managebale feature. From looking at the pool digestEvent methods looks like it should not be a porblem as long as we handle the "getHash" func.
- Maybe we don't need the "Message" struct, EventBatch can contain more data besides an array of Events.
- Look what is the specific structure of the SGLang events and summerize here. (overall, looks similar, just the actual Event objects have differend fields)
- Go over the flow of creating the subsribers.
- Bind/Connect remains subscriber's responsibility. Trsnaport will just need to implement both options.

