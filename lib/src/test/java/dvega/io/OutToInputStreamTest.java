package dvega.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import dvega.io.OutToInputStream.OutStreamException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class OutToInputStreamTest {
	@Test
	public void normalUsage() throws IOException {
		final int total = 10;
		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out);
			for (int i = 0; i < total; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) break;
			}
			pw.close();
		};

		long count;
		try (var is = OutToInputStream.asInputStream(generator)) {
			count = new BufferedReader(new InputStreamReader(is)).lines().count();
		}
		assertEquals(total, count);
	}

	@Test
	public void normalUsageBytes() throws IOException {
		final int total = 10;
		OutToInputStream.OutputProducer generator = out -> {
			for (int i = 0; i < total; i++) {
				out.write(i);
			}
		};

		try (var is = OutToInputStream.asInputStream(generator)) {
			for (int i = 0; i < total; i++) {
				assertEquals(i, is.read());
			}
			assertEquals(0, is.available());
			assertEquals(-1, is.read());
			assertEquals(0, is.available());
		}
	}

	@Test
	public void zeroBytes() throws IOException {
		try (var is = OutToInputStream.asInputStream(out -> out.write(new byte[0]))) {
			int nread;
			nread = is.read(new byte[0]);
			assertEquals(0, nread);

			assertEquals(-1, is.read());

			nread = is.read(new byte[0]);
			assertEquals(0, nread);
		}
	}

	@Test
	public void readAfterClose() throws IOException {
		final int total = 10;
		OutToInputStream.OutputProducer generator = out -> {
			for (int i = 0; i < total; i++) {
				out.write(i);
			}
			out.close();
			out.close();
		};

		var is = OutToInputStream.asInputStream(generator);
		is.readAllBytes();
		is.close();
		is.close();
		is.close();
		var ex = assertThrows(IOException.class, is::read);
		assertEquals("Receiver stream is closed", ex.getMessage());
	}

	@Test
	public void writeAfterClose() {
		final int total = 10;
		OutToInputStream.OutputProducer generator = out -> {
			for (int i = 0; i < total; i++) {
				out.write(i);
			}
			out.close();
			out.close();
			out.write(65);
		};

		var is = OutToInputStream.asInputStream(generator);
		var ex = assertThrows(OutStreamException.class, is::readAllBytes);
		assertEquals("Output stream closed", ex.getMessage());
	}


	@Test
	public void outIOException() throws IOException {
		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out);
			for (int i = 0; i < 10; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) return;
			}
			throw new IOException("out error");
		};

		try (var is = OutToInputStream.asInputStream(generator)) {
			var ex = assertThrows(OutStreamException.class, is::readAllBytes);
			assertEquals("out error", ex.getMessage());
		}
	}

	@Test
	public void outRuntimeException() throws IOException {
		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out);
			for (int i = 0; i < 10; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) return;
			}
			throw new IllegalArgumentException("out error");
		};

		try (var is = OutToInputStream.asInputStream(generator)) {
			var ex = assertThrows(OutStreamException.class, is::readAllBytes);
			assertEquals("java.lang.IllegalArgumentException: out error", ex.getMessage());
		}
	}

	@Test
	public void inAbandon() throws IOException, InterruptedException {
		var countDownLatch = new CountDownLatch(1);
		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out);
			for (int i = 0; i < 1000000; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) {
					countDownLatch.countDown();
					throw new IOException("unexpected");
				}
			}
		};

		var is = OutToInputStream.asInputStream(generator);
		var br = new BufferedReader(new InputStreamReader(is));
		br.readLine();
		br.readLine();
		br.readLine();
		br = null; is = null; // Mark for GC

		for (int i = 0; i < 5; i++) {
			System.gc();
			if (countDownLatch.await(1, TimeUnit.SECONDS)) return;
		}
		fail("Out didn't end in time");
	}

	@Test
	public void outInterrupt() throws IOException {
		AtomicReference<Thread> outThread = new AtomicReference<>();
		OutToInputStream.OutputProducer generator = out -> {
			outThread.set(Thread.currentThread());
			var writer = new OutputStreamWriter(out);
			for (int i = 0; i < 1000000; i++) {
				writer.write("i=" + i + "\n");
			}
		};

		var is = OutToInputStream.asInputStream(generator);
		assertTrue(is.read() != -1);
		outThread.get().interrupt();
		var ex = assertThrows(OutStreamException.class, is::readAllBytes);
		assertEquals(InterruptedIOException.class, ex.getCause().getClass());
	}

	@Test
	public void inInterrupt() {
		AtomicReference<Thread> inThread = new AtomicReference<>(Thread.currentThread());

		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out, true);
			for (int i = 0; i < 1000000; i++) {
				pw.println("i=" + i + "\n");
				if (i == 2) inThread.get().interrupt();
			}
		};

		var is = OutToInputStream.asInputStream(generator);
		assertThrows(InterruptedIOException.class, is::readAllBytes);
	}

	@Test
	public void inClosed() throws IOException, InterruptedException {
		var countDownLatch = new CountDownLatch(1);
		OutToInputStream.OutputProducer generator = out -> {
			var pw = new PrintWriter(out);
			for (int i = 0; i < 1000000; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) {
					countDownLatch.countDown();
					return;
				}
			}
		};

		var is = OutToInputStream.asInputStream(generator);
		for (int ch; (ch = is.read()) != -1 && ch != '\n';);
		assertTrue(is.read() >= 0);
		assertTrue(is.available() > 0);
		is.close();

		if (countDownLatch.await(5, TimeUnit.SECONDS)) return;
		fail("Out didn't end in time");
	}


	@Test
	public void nested() throws IOException {
		var is0 = OutToInputStream.asInputStream(out -> {
			var pw = new PrintWriter(out, true);
			for (int i = 0; i < 100; i++) {
				pw.println("i=" + i);
				if (i == 3) throw new IOException("error");
			}
		});

		var isi = is0;
		for (int i = 0; i < 10; i++) {
			var is = isi;
			isi = OutToInputStream.asInputStream(is::transferTo);
		}

		var is = isi;
		var ex = assertThrows(OutStreamException.class, () -> is.transferTo(System.out));
		assertEquals("error", ex.getMessage());
		isi.close();
	}

	@Test
	public void nestedClosed() throws IOException, InterruptedException {
		var countDownLatch = new CountDownLatch(1);
		var isi = OutToInputStream.asInputStream(out -> {
			var pw = new PrintWriter(out, true);
			for (int i = 0; i < 100; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) {
					countDownLatch.countDown();
					return;
				}
			}
		});

		for (int i = 0; i < 10; i++) {
			var is = isi;
			isi = OutToInputStream.asInputStream(out -> {
				try (is) {
					is.transferTo(out);
				}
			});
		}

		var pr = new BufferedReader(new InputStreamReader(isi));
		pr.readLine();
		pr.readLine();
		pr.readLine();
		pr.close();
		countDownLatch.await();

	}

	@Test
	public void nestedAbandon() throws IOException, InterruptedException {
		var countDownLatch = new CountDownLatch(1);
		var isi = OutToInputStream.asInputStream(out -> {
			var pw = new PrintWriter(out, true);
			for (int i = 0; i < 100; i++) {
				pw.println("i=" + i);
				if (pw.checkError()) {
					countDownLatch.countDown();
					return;
				}
			}
		});

		for (int i = 0; i < 10; i++) {
			var is = isi;
			isi = OutToInputStream.asInputStream(is::transferTo);
		}

		var pr = new BufferedReader(new InputStreamReader(isi));
		pr.readLine();
		pr.readLine();
		pr.readLine();
		pr = null; isi = null; // Mark for GC

		for (int i = 0; i < 500; i++) {
			System.gc();
			if (countDownLatch.await(5, TimeUnit.MILLISECONDS)) return;
		}
		fail("Out didn't end in time");
	}

	@Test
	public void base64wrap() throws IOException {
		Supplier<InputStream> fibSupplier = () -> new InputStream() {
			int f0 = 0;
			int f1 = 1;
			@Override
			public int read() {
				int ret;
				f1 = (ret = f0) + (f0 = f1);
				return ret & 0xFF;
			}
		};

		var is0 = Base64.getDecoder().wrap(OutToInputStream.asInputStream(out ->
				fibSupplier.get().transferTo(Base64.getEncoder().wrap(out))));
		var is1 = fibSupplier.get();

		for (int i = 0; i < 100; i++) {
			assertEquals(is1.read(), is0.read());
		}
	}
}
