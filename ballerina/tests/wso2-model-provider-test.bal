// Copyright (c) 2026 WSO2 LLC (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/test;

const int MOCK_CHAT_PORT = 9096;
const MOCK_CHAT_URL = "http://localhost:9096";

const MOCK_CHAT_TEXT_RESPONSE = "Hello! How can I help you today?";

const string TRIGGER_STREAM_ERROR = "trigger-stream-error";
const string TRIGGER_COMBINED_STREAM_CHUNK = "trigger-combined-chunk";

// Streams the SSE events collected in `events` one at a time.
class MockSseEventIterator {
    private final http:SseEvent[] events;
    private int index = 0;

    function init(http:SseEvent[] events) {
        self.events = events;
    }

    public isolated function next() returns record {|http:SseEvent value;|}|error? {
        lock {
            if self.index >= self.events.length() {
                return ();
            }
            http:SseEvent event = self.events[self.index];
            self.index += 1;
            return {value: event};
        }
    }

    public isolated function close() returns error? {
        return ();
    }
}

// Splits `MOCK_CHAT_TEXT_RESPONSE` across a couple of content deltas, then a finish chunk.
function mockTextStreamEvents() returns http:SseEvent[] => [
    {data: string `{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{"content":"Hello! "}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{"content":"How can I help you today?"}}]}`},
    {
        data: string `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],` +
            string `"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`
    },
    {data: "[DONE]"}
];

// A single wire chunk carrying a content fragment and a finish reason together, as some
// OpenAI-compatible gateways emit. One raw chunk, two normalized chunks.
function mockCombinedStreamEvents() returns http:SseEvent[] => [
    {
        data: string `{"id":"resp-combined","choices":[{"index":0,` +
            string `"delta":{"role":"assistant","content":"All done."},"finish_reason":"stop"}]}`
    },
    {data: "[DONE]"}
];

// Splits a `searchFunction({"query":"test"})` call's name and arguments across a few deltas.
function mockFunctionCallStreamEvents() returns http:SseEvent[] => [
    {data: string `{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{"function_call":{"name":"searchFunction"}}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{"function_call":{"arguments":"{\"query\":"}}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{"function_call":{"arguments":"\"test\"}"}}}]}`},
    {data: string `{"choices":[{"index":0,"delta":{},"finish_reason":"function_call"}]}`},
    {data: "[DONE]"}
];

// Mock intelligence service for Wso2ModelProvider tests.
// Returns a function-call response when the request contains `functions`, otherwise a plain text response.
// When the request has `stream: true`, responds with SSE chunks instead of a single JSON body; a user
// message containing `TRIGGER_STREAM_ERROR` makes the streaming path fail with a 500 to exercise error handling.
service on new http:Listener(MOCK_CHAT_PORT) {

    resource function post chat/completions(@http:Payload json payload, @http:Header string Authorization)
    returns json|stream<http:SseEvent, error?>|http:InternalServerError|error {
        if Authorization != "Bearer test-token" {
            return error("invalid authorization token");
        }
        json|error functions = payload.functions;
        boolean isFunctionCall = functions is json[] && functions.length() > 0;

        json|error streamFlag = payload.'stream;
        boolean isStreamRequest = streamFlag is boolean && streamFlag;

        if isStreamRequest {
            json|error messages = payload.messages;
            if messages is json[] && messages.length() > 0 && messages.toString().includes(TRIGGER_STREAM_ERROR) {
                return <http:InternalServerError>{body: {message: "simulated streaming failure"}};
            }
            if messages is json[] && messages.toString().includes(TRIGGER_COMBINED_STREAM_CHUNK) {
                return new stream<http:SseEvent, error?>(new MockSseEventIterator(mockCombinedStreamEvents()));
            }
            http:SseEvent[] events = isFunctionCall ? mockFunctionCallStreamEvents() : mockTextStreamEvents();
            return new stream<http:SseEvent, error?>(new MockSseEventIterator(events));
        }

        if isFunctionCall {
            return {
                id: "resp-func-call",
                'object: "chat.completion",
                created: 1700000000,
                model: "gpt-4o-mini",
                choices: [
                    {
                        index: 0,
                        message: {
                            role: "assistant",
                            content: (),
                            function_call: {
                                name: "searchFunction",
                                arguments: "{\"query\":\"test\"}"
                            }
                        },
                        finish_reason: "function_call"
                    }
                ],
                usage: {prompt_tokens: 5, completion_tokens: 10, total_tokens: 15}
            };
        }
        return {
            id: "resp-text",
            'object: "chat.completion",
            created: 1700000000,
            model: "gpt-4o-mini",
            choices: [
                {
                    index: 0,
                    message: {
                        role: "assistant",
                        content: MOCK_CHAT_TEXT_RESPONSE
                    },
                    finish_reason: "stop"
                }
            ],
            usage: {prompt_tokens: 5, completion_tokens: 10, total_tokens: 15}
        };
    }
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatWithUserMessage() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    ChatAssistantMessage response = check provider->chat({role: USER, content: "Hello"}, []);
    test:assertEquals(response.role, ASSISTANT);
    test:assertEquals(response.content, MOCK_CHAT_TEXT_RESPONSE);
    test:assertTrue(response.toolCalls is (), "Expected no tool calls for plain text response");
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatWithMultipleMessages() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    ChatMessage[] messages = [
        {role: SYSTEM, content: "You are a helpful assistant."},
        {role: USER, content: "Hello"}
    ];
    ChatAssistantMessage response = check provider->chat(messages, []);
    test:assertEquals(response.role, ASSISTANT);
    test:assertEquals(response.content, MOCK_CHAT_TEXT_RESPONSE);
    test:assertTrue(response.toolCalls is (), "Expected no tool calls");
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatWithCustomTemperature() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token", temperature = 0.2d);
    ChatAssistantMessage response = check provider->chat({role: USER, content: "Hello"}, []);
    test:assertEquals(response.role, ASSISTANT);
    test:assertEquals(response.content, MOCK_CHAT_TEXT_RESPONSE);
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatWithTools() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    ChatCompletionFunctions[] tools = [
        {
            name: "searchFunction",
            description: "Search for information",
            parameters: {
                'type: "object",
                properties: {query: {'type: "string"}}
            }
        }
    ];
    ChatAssistantMessage response = check provider->chat({role: USER, content: "Search for test"}, tools);
    test:assertEquals(response.role, ASSISTANT);
    FunctionCall[]? toolCalls = response.toolCalls;
    if toolCalls is () || toolCalls.length() == 0 {
        test:assertFail("Expected tool calls in the response");
    }
    test:assertEquals(toolCalls[0].name, "searchFunction");
    map<json>? args = toolCalls[0].arguments;
    if args is () {
        test:assertFail("Expected arguments in tool call");
    }
    test:assertEquals(args["query"], "test");
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatStreamWithUserMessage() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    stream<ChatCompletionChunk, Error?> chunkStream = check provider->chatStream({role: USER, content: "Hello"}, []);

    string content = "";
    int textChunkCount = 0;
    int stopChunkCount = 0;
    FinishReason? lastFinishReason = ();
    while true {
        record {|ChatCompletionChunk value;|}|Error? next = chunkStream.next();
        if next is () {
            break;
        }
        if next is Error {
            test:assertFail("Unexpected error while streaming: " + next.message());
        }
        ChatCompletionChunk chunk = next.value;
        if chunk is ChatCompletionTextChunk {
            content += chunk.content;
            textChunkCount += 1;
        } else if chunk is ChatCompletionStopChunk {
            lastFinishReason = chunk.finishReason;
            stopChunkCount += 1;
        } else {
            test:assertFail("Unexpected chunk kind in a plain text stream");
        }
    }

    test:assertEquals(content, MOCK_CHAT_TEXT_RESPONSE);
    // The opening role-only delta and the trailing usage carry no update, so neither
    // reaches the caller: one chunk per content delta, plus the terminal chunk.
    test:assertEquals(textChunkCount, 2);
    test:assertEquals(stopChunkCount, 1);
    test:assertEquals(lastFinishReason, STOP);
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatStreamFansOutCombinedChunk() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    stream<ChatCompletionChunk, Error?> chunkStream =
        check provider->chatStream({role: USER, content: TRIGGER_COMBINED_STREAM_CHUNK}, []);

    ChatCompletionChunk[] chunks = [];
    while true {
        record {|ChatCompletionChunk value;|}|Error? next = chunkStream.next();
        if next is () {
            break;
        }
        if next is Error {
            test:assertFail("Unexpected error while streaming: " + next.message());
        }
        chunks.push(next.value);
    }

    // One wire chunk carried both a content fragment and a finish reason; each kind of
    // update is delivered as its own chunk, text first.
    test:assertEquals(chunks.length(), 2);
    ChatCompletionChunk first = chunks[0];
    if first !is ChatCompletionTextChunk {
        test:assertFail("Expected the text fragment to be delivered first");
    }
    test:assertEquals(first.content, "All done.");
    ChatCompletionChunk second = chunks[1];
    if second !is ChatCompletionStopChunk {
        test:assertFail("Expected the finish reason to be delivered as a terminal chunk");
    }
    test:assertEquals(second.finishReason, STOP);
    // The completion id is stable across every chunk fanned out from one raw chunk.
    test:assertEquals(first?.id, "resp-combined");
    test:assertEquals(second?.id, "resp-combined");
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatStreamWithTools() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    ChatCompletionFunctions[] tools = [
        {
            name: "searchFunction",
            description: "Search for information",
            parameters: {
                'type: "object",
                properties: {query: {'type: "string"}}
            }
        }
    ];
    stream<ChatCompletionChunk, Error?> chunkStream =
        check provider->chatStream({role: USER, content: "Search for test"}, tools);

    string accumulatedName = "";
    string accumulatedArguments = "";
    int stopChunkCount = 0;
    FinishReason? lastFinishReason = ();
    while true {
        record {|ChatCompletionChunk value;|}|Error? next = chunkStream.next();
        if next is () {
            break;
        }
        if next is Error {
            test:assertFail("Unexpected error while streaming: " + next.message());
        }
        ChatCompletionChunk chunk = next.value;
        if chunk is ChatCompletionToolCallChunk {
            ToolCallFragment[] toolCalls = chunk.toolCalls;
            test:assertEquals(toolCalls.length(), 1);
            test:assertEquals(toolCalls[0].index, 0, "Fragments of one call share an index");
            string? name = toolCalls[0]?.name;
            if name is string {
                accumulatedName += name;
            }
            string? args = toolCalls[0]?.arguments;
            if args is string {
                accumulatedArguments += args;
            }
        } else if chunk is ChatCompletionStopChunk {
            lastFinishReason = chunk.finishReason;
            stopChunkCount += 1;
        } else {
            test:assertFail("Unexpected chunk kind in a tool call stream");
        }
    }

    test:assertEquals(accumulatedName, "searchFunction");
    map<json> parsedArguments = check accumulatedArguments.fromJsonStringWithType();
    test:assertEquals(parsedArguments["query"], "test");
    test:assertEquals(stopChunkCount, 1);
    test:assertEquals(lastFinishReason, TOOL_CALLS);
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderChatStreamConnectionError() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    stream<ChatCompletionChunk, Error?>|Error result =
        provider->chatStream({role: USER, content: TRIGGER_STREAM_ERROR}, []);
    test:assertTrue(result is Error, "Expected an error when the streaming connection fails");
}

@test:Config {
    groups: ["wso2-model-provider"]
}
function testWso2ModelProviderGenerateStreamWithUserMessage() returns error? {
    Wso2ModelProvider provider = check new (MOCK_CHAT_URL, "test-token");
    stream<string, Error?> textStream = check provider->generateStream(`Hello`);

    string content = "";
    while true {
        record {|string value;|}|Error? next = textStream.next();
        if next is () {
            break;
        }
        if next is Error {
            test:assertFail("Unexpected error while streaming: " + next.message());
        }
        content += next.value;
    }
    test:assertEquals(content, MOCK_CHAT_TEXT_RESPONSE);
}
