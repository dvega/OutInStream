package dvega.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Creates an OutputStream connected to an InputStream.
 * <p>Data written in the output is available on the input. This is useful for scenarios where data is produced
 * into an OutputStream but needs to be consumed from an InputStream.
 * <p>Similar to {@link java.io.PipedInputStream PipedInputStream}/{@link java.io.PipedOutputStream PipedOutputStream}
 * but errors are communicated properly between ends
 */
public class OutToInputStream {
	private static final System.Logger LOGGER = System.getLogger(OutToInputStream.class.getPackageName());
	private static Reference<Cleaner> cleanerReference;

	public interface OutputProducer {
		void accept(OutputStream out) throws Exception;
	}

	public static class OutStreamException extends IOException {
		private OutStreamException(Throwable cause) {
			super(cause instanceof IOException ? cause.getMessage() : cause.toString(), cause);
		}
	}

	private static void locked(ReentrantLock lock, Runnable other) {
		lock.lock();
		try {
			other.run();
		} finally {
			lock.unlock();
		}
	}

	private static synchronized Cleaner internalCleaner() {
		Cleaner cleaner;
		var ref = cleanerReference;
		if (ref == null || (cleaner = ref.get()) == null)
			cleanerReference = new WeakReference<>(cleaner = Cleaner.create());
		return cleaner;
	}

	/**
	 * Creates an InputStream that reads data written to an OutputStream provided to the producer.
	 * The producer will be executed in a separate thread, writing data to an internal OutputStream,
	 * which is then made available through the returned InputStream.
	 *
	 * @param producer A functional interface that accepts an OutputStream. The producer should
	 *                 write all its data to this OutputStream.
	 * @param executor The Executor service that will start the producer thread. If {@code null},
	 *                 a new thread will be created.
	 * @param cleaner  The Cleaner to use to clean up the service. If {@code null}, an internal
	 *                 cleaner instance is created
	 * @return An InputStream that provides the data written by the producer to its OutputStream.
	 */
	public static InputStream asInputStream(OutputProducer producer, Executor executor, Cleaner cleaner) {
		var lock = new ReentrantLock();
		var bufferEmpty = lock.newCondition();
		var bufferFull = lock.newCondition();

		var channel = new Object() {
			/*
			    buffer == null -> IS is waiting for data
			    buffer.length == 0 -> OS is closed
			    pos == -1 --> IS is closed
			    exp != null --> IO exception generated from OS
			 */
			private byte[] buffer;
			private boolean softClose;
			private int pos;
			private int length;
			private Exception exp;

			private void checkISNotClosed() throws IOException {
				if (pos < 0) throw new IOException("Receiver stream is closed");
			}

			public int available() throws IOException {
				checkISNotClosed();
				var buff = buffer;
				if (buff == null || buff.length == 0) return 0;
				return length - pos;
			}

			public void closeIS() {
				pos = -1;
				bufferEmpty.signalAll();
			}

			private void checkOSNotClosed() throws IOException {
				if (isOSClosed()) throw new IOException("Output stream closed");
			}
			private void supply(byte[] bytes, int off, int len) throws IOException {
				waitBufferConsumed();
				checkISNotClosed();
				buffer = bytes;
				pos = off;
				length = off + len;
				bufferFull.signalAll();
			}

			private void waitBufferConsumed() throws IOException {
				while (true) {
					checkOSNotClosed();
					if (buffer == null) return;
					checkISNotClosed();
					try {
						bufferEmpty.await();
					} catch (InterruptedException e) {
						throw new InterruptedIOException();
					}
				}
			}

			private boolean isOSClosed() {
				if (softClose) return true;
				var buff = buffer;
				return (buff != null && buff.length == 0);
			}

			private void flush() throws IOException {
				waitBufferConsumed();
			}

			private void softCloseOS(Exception original) {
				if (isOSClosed()) return;
				try {
					flush();
				} catch (IOException e) {
					if (original != null) original.addSuppressed(e);
					else LOGGER.log(Level.ERROR, "Flush error", e);
				}
				softClose = true;
			}

			private void closeOS(Exception exception) {
				softCloseOS(exception);
				if (exception != null && pos < 0) {
					LOGGER.log(Level.ERROR, "OutStream finish with error", exception);
					return;
				}
				exp = exception;
				buffer = new byte[0];
				bufferFull.signalAll();
			}

			private byte[] waitForBuffer() throws IOException {
				while (true) {
					checkISNotClosed();
					var buf = buffer; Exception e;
					if ((buf == null || buf.length == 0) && (e = exp) != null) {
						throw new OutStreamException(e);
					}
					if (buf != null) return buf;
					try {
						bufferFull.await();
					} catch (InterruptedException ignore) {
						throw new InterruptedIOException();
					}
				}
			}

			private void checkMarkEmpty() {
				if (pos < length) return;
				pos = 0;
				buffer = null;
				bufferEmpty.signalAll();
			}

			public int read() throws IOException {
				byte[] buf = waitForBuffer();
				if (buf.length == 0) return -1;
				byte b = buf[pos++];
				checkMarkEmpty();
				return b & 0xFF;
			}

			public int read(byte[] b, int off, int len) throws IOException {
				byte[] buf = waitForBuffer();
				if (buf.length == 0) return -1;
				int l = Math.min(length - pos, len);
				System.arraycopy(buf, pos, b, off, l);
				pos += l;
				checkMarkEmpty();
				return l;
			}
		};

		if (executor == null) {
			executor =  r -> {
				var t = new Thread(r);
				t.setName(Thread.currentThread().getName() + "-OutStream");
				t.setDaemon(true);
				t.start();
			};
		}

		executor.execute(() -> {
			var out = new OutputStream() {
				@Override
				public void write(int b) throws IOException {
					lock.lock();
					try {
						channel.supply(new byte[]{(byte) b}, 0, 1);
					} finally {
						lock.unlock();
					}
				}

				@Override
				public void flush() throws IOException {
					lock.lock();
					try {
						channel.flush();
					} finally {
						lock.unlock();
					}
				}

				@Override
				public void write(byte[] b, int off, int len) throws IOException {
					Objects.checkFromIndexSize(off, len, b.length);
					if (len == 0) return;
					lock.lock();
					try {
						channel.supply(b, off, len);
						channel.flush();
					} finally {
						lock.unlock();
					}
				}

				@Override
				public void close() {
					locked(lock, () -> channel.softCloseOS(null));
				}

				void reallyClose(Exception exp) {
					locked(lock, () -> channel.closeOS(exp));
				}
			};

			LOGGER.log(Level.DEBUG, "OutStream Thread started");
			Exception exp = null;
			try {
				producer.accept(out);
			} catch (Exception e) {
				exp = e;
			} finally {
				out.reallyClose(exp);
			}
			LOGGER.log(Level.DEBUG, "OutStream Thread ended");
		});

		Cleaner theCleaner = cleaner == null ? internalCleaner() : cleaner;

		return new InputStream() {
			private final Cleaner.Cleanable cleanable = theCleaner.register(this, () -> locked(lock, channel::closeIS));

			@Override
			public int read() throws IOException {
				lock.lock();
				try {
					return channel.read();
				} finally {
					lock.unlock();
				}
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				Objects.checkFromIndexSize(off, len, b.length);
				if (len==0) return 0;
				lock.lock();
				try {
					return channel.read(b, off, len);
				} finally {
					lock.unlock();
				}
			}

			@Override
			public int available() throws IOException {
				lock.lock();
				try {
					return channel.available();
				} finally {
					lock.unlock();
				}
			}

			@Override
			public void close() {
				cleanable.clean();
			}
		};
	}

	/**
	 * Creates an InputStream that reads data written to an OutputStream provided to the producer.
	 * The producer will be executed in a separate thread, writing data to an internal OutputStream,
	 * which is then made available through the returned InputStream.
	 *
	 * @param producer A functional interface that accepts an OutputStream. The producer should
	 *                 write all its data to this OutputStream.
	 * @return An InputStream that provides the data written by the producer to its OutputStream.
	 * @see #asInputStream(OutputProducer, Executor, Cleaner)  
	 */
	public static InputStream asInputStream(OutputProducer producer) {
		return asInputStream(producer, null, null);
	}
}
