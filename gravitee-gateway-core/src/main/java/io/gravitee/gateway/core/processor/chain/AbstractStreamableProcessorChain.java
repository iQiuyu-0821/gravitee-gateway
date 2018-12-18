/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.gateway.core.processor.chain;

import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.stream.ReadStream;
import io.gravitee.gateway.api.stream.WriteStream;
import io.gravitee.gateway.core.processor.ProcessorFailure;
import io.gravitee.gateway.core.processor.StreamableProcessor;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public abstract class AbstractStreamableProcessorChain<T, S, H extends StreamableProcessor<T, S>> extends AbstractProcessorChain<T, H, H>
        implements StreamableProcessorChain<T, S, H> {

    private H streamableProcessorChain;
    private H previousProcessor;
    protected Handler<ProcessorFailure> streamErrorHandler;

    @Override
    public void handle(T data) {
        if (hasNext()) {
            H processor = next(data);
            if (streamableProcessorChain == null) {
                streamableProcessorChain = processor;
            }

            // Chain processor stream using the previous one
            if (previousProcessor != null) {
                previousProcessor.bodyHandler(processor::write);
                previousProcessor.endHandler(result1 -> processor.end());
            }

            processor
                    .handler(__ -> handle(data))
                    .errorHandler(failure -> errorHandler.handle(failure))
                    .exitHandler(stream -> exitHandler.handle(null))
                    .handle(data);

            // Previous stream is now the current processor stream
            previousProcessor = processor;
        } else {
            doOnSuccess(data);
        }
    }

    @Override
    public ReadStream<S> bodyHandler(Handler<S> handler) {
        return streamableProcessorChain.bodyHandler(handler);
    }

    @Override
    public ReadStream<S> endHandler(Handler<Void> handler) {
        return streamableProcessorChain.endHandler(handler);
    }

    @Override
    public WriteStream<S> write(S chunk) {
        return streamableProcessorChain.write(chunk);
    }

    @Override
    public void end() {
        streamableProcessorChain.end();
    }

    @Override
    public StreamableProcessorChain<T, S, H> handler(Handler<H> handler) {
        this.resultHandler = handler;
        return this;
    }

    @Override
    public StreamableProcessorChain<T, S, H> errorHandler(Handler<ProcessorFailure> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    @Override
    public StreamableProcessorChain<T, S, H> exitHandler(Handler<Void> exitHandler) {
        this.exitHandler = exitHandler;
        return this;
    }

    @Override
    public StreamableProcessorChain<T, S, H> streamErrorHandler(Handler<ProcessorFailure> handler) {
        this.streamErrorHandler = handler;
        return this;
    }

    @Override
    public void doOnSuccess(T data) {
        resultHandler.handle(streamableProcessorChain);
    }
}
