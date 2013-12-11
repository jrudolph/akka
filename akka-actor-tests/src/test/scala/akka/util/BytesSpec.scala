package akka.util

import java.io.{ FileOutputStream, File }
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.{ Gen, Arbitrary }
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

class BytesSpec extends WordSpec with MustMatchers with Checkers {
  import BytesUtils._

  def genFileByte(min: Int, max: Int): Gen[Bytes] =
    for {
      n ← choose(min, max)
    } yield FileBytes("/tmp/dummy", 0, n) // doesn't really have to exist
  def genSimpleByteString(min: Int, max: Int) = for {
    n ← choose(min, max)
    b ← Gen.containerOfN[Array, Byte](n, arbitrary[Byte])
    from ← choose(0, b.length)
    until ← choose(from, b.length)
  } yield ByteString(b).slice(from, until)

  implicit val arbitraryBytes: Arbitrary[Bytes] = Arbitrary {
    Gen.sized { s ⇒
      for {
        n ← choose(0, s)
        element ← Gen.oneOf(genSimpleByteString(0, 1000), genFileByte(0, 1000))
        elements ← listOfN(n, element)
      } yield ((ByteString.empty: Bytes) /: elements)(_ ++ _)
    }
  }

  "Bytes" must {
    "properly support `copyToArray`" when {
      "ByteString" in {
        testCopyToArray(ByteString("Ken sent me!"))
      }
      "Bytes.FileBytes" in {
        withFileBytes()(testCopyToArray)
      }
      "Bytes.CompoundBytes just with ByteStrings" in {
        testCopyToArray(Bytes("Ken") ++ ByteString(" sent") ++ ByteString(" me") ++ ByteString("!"))
      }
      "Bytes.CompoundBytes containing ByteString and FileBytes" in {
        withFileBytes(" sent")(fb ⇒ testCopyToArray(Bytes("Ken") ++ fb ++ ByteString(" me") ++ ByteString("!")))
      }
    }
    "properly support `sliceBytes`" when {
      "ByteString" in {
        ByteString("Ken sent me!").sliceBytes(4, 3) must be === ByteString("sen")
      }
      "Bytes.FileBytes" in {
        withFileBytes("Ken sent me!")(_.sliceBytes(4, 3) must be === ByteString("sen"))
      }
      "Bytes.CompoundBytes just with ByteStrings" in {
        val data = ByteString("Ken") ++ ByteString(" sent ") ++ ByteString("me") ++ ByteString("!")
        data.sliceBytes(2, 5) must be === ByteString("n sen")
      }
      "Bytes.CompoundBytes containing ByteString and FileBytes" in {
        withFileBytes(" sent") { fb ⇒
          val data = ByteString("Ken") ++ fb ++ ByteString("me") ++ ByteString("!")
          data.sliceBytes(2, 5) must be === ByteString("n sen")
        }
      }
    }
    "properly support `slice`" when {
      "ByteString" in {
        ByteString("Ken sent me!").slice(4L, 3L) must be === ByteString("sen")
      }
      "Bytes.FileBytes" in {
        val file = File.createTempFile("akka-util_BytesSpec", ".txt")
        try {
          writeAllText(" Ken sent me!", file)
          Bytes(file, 1).slice(4L, 3L) must be === Bytes(file, 5, 3)
        } finally file.delete
      }
      "Bytes.CompoundBytes just with ByteStrings" in {
        val data = ByteString("Ken") ++ ByteString(" sent ") ++ ByteString("me") ++ ByteString("!")
        data.slice(2L, 5L) must be === (Bytes("n") ++ ByteString(" sen"))
      }
      "Bytes.CompoundBytes containing ByteString and FileBytes" in {
        withFileBytes(" sent ") { fb ⇒
          val data = ByteString("Ken") ++ fb ++ ByteString("me") ++ ByteString("!")
          data.slice(2L, 5L) must be === (ByteString("n") ++ fb.slice(0, 4))
        }
      }
    }
    "properly support `toChunkStream`" when {
      "ByteString" in {
        ByteString("Ken sent me!").toChunkStream(5) must be === Stream(
          ByteString("Ken s"),
          ByteString("ent m"),
          ByteString("e!"))
      }
      "Bytes.FileBytes" in {
        withFileBytes("Ken sent me!") { fb ⇒
          fb.toChunkStream(5) must be === Stream(
            fb.slice(0, 5),
            fb.slice(5, 5),
            fb.slice(10, 2))
        }
      }
      "Bytes.CompoundBytes just with ByteStrings" in {
        val data = ByteString("Ken") ++ ByteString(" sent ") ++ ByteString("me!")
        data.toChunkStream(5) must be === Stream(
          ByteString("Ken s"),
          ByteString("ent m"),
          ByteString("e!"))
      }
      "Bytes.CompoundBytes containing ByteString and FileBytes" in {
        withFileBytes(" sent ") { fb ⇒
          val data = ByteString("Ken") ++ fb ++ ByteString("me!")
          data.toChunkStream(5) must be === Stream(
            ByteString("Ken") ++ fb.slice(0, 2),
            fb.slice(2, 4) ++ ByteString("m"),
            ByteString("e!"))
        }
      }
    }
    "properly support `toByteString`" when {
      "ByteString" in {
        val bytes = ByteString("Ken sent me!").toByteString
        bytes.isCompact must be === true
        bytes must be === ByteString("Ken sent me!")
      }
      "Bytes.FileBytes" in {
        val file = File.createTempFile("spray-http_BytesSpec", ".txt")
        try {
          writeAllText("Ken sent me!", file)
          Bytes(file).toByteString must be === ByteString("Ken sent me!")

        } finally file.delete
      }
      "Bytes.CompoundBytes just with ByteStrings" in {
        val data = ByteString("Ken") ++ ByteString(" sent ") ++ ByteString("me") ++ ByteString("!")
        data.toByteString must be === ByteString("Ken sent me!")
      }
      "Bytes.CompoundBytes containing ByteString and FileBytes" in {
        withFileBytes(" sent ") { fb ⇒
          val data = ByteString("Ken") ++ fb ++ ByteString("me") ++ ByteString("!")
          data.toByteString must be === ByteString("Ken sent me!")
        }
      }
    }
    "support `++`" when {
      "FileBytes ++ consecutive FileBytes" in {
        withFileBytes("a" * 1024) { fb ⇒
          fb.slice(0, 20) ++ fb.slice(20, 30) must be === fb.slice(0, 50)
        }
      }

      "size" in {
        check { (a: Bytes, b: Bytes) ⇒
          (a ++ b).longLength == a.longLength + b.longLength
        }
      }
      "be sequential" in {
        check { (a: Bytes, b: Bytes) ⇒
          (a ++ b).slice(a.longLength) == b
        }
      }
    }
  }

  def testCopyToArray(data: Bytes): Unit = {
    testCopyToArray(data, sourceOffset = 0, targetOffset = 0, span = 12) must be === "Ken sent me!xxxx"
    testCopyToArray(data, sourceOffset = 0, targetOffset = 2, span = 12) must be === "xxKen sent me!xx"
    testCopyToArray(data, sourceOffset = 0, targetOffset = 4, span = 12) must be === "xxxxKen sent me!"
    testCopyToArray(data, sourceOffset = 0, targetOffset = 6, span = 12) must be === "xxxxxxKen sent m"
    testCopyToArray(data, sourceOffset = 2, targetOffset = 0, span = 12) must be === "n sent me!xxxxxx"
    testCopyToArray(data, sourceOffset = 8, targetOffset = 0, span = 12) must be === " me!xxxxxxxxxxxx"
    testCopyToArray(data, sourceOffset = 8, targetOffset = 10, span = 2) must be === "xxxxxxxxxx mxxxx"
    testCopyToArray(data, sourceOffset = 8, targetOffset = 10, span = 8) must be === "xxxxxxxxxx me!xx"
  }

  def testCopyToArray(data: Bytes, sourceOffset: Long, targetOffset: Int, span: Int): String = {
    val array = "xxxxxxxxxxxxxxxx".getBytes
    data.copyToArray(array, sourceOffset, targetOffset, span)
    new String(array)
  }
}

object BytesUtils {
  def withFileBytes[T](text: String = "Ken sent me!")(f: Bytes ⇒ T): T = {
    val file = File.createTempFile("akka-util_BytesSpec", ".txt")
    try {
      writeAllText(text, file)
      f(Bytes(file))
    } finally file.delete
  }
  def writeAllText(text: String, file: File) = {
    val fos = new FileOutputStream(file)
    try fos.write(text.getBytes("utf8"))
    finally fos.close()
  }
}
