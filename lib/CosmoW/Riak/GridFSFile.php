<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace CosmoW\Riak;

/**
 * Wrapper for the RiakGridFSFile class.
 *
 * Files may be dirty, which means that they must be persisted to the database.
 * Clean files are assumed to be in sync with the database.
 *
 * @since  1.0
 * @author Jonathan H. Wage <jonwage@gmail.com>
 */
class GridFSFile
{
    /**
     * The PHP RiakGridFSFile instance being wrapped.
     *
     * @var \RiakGridFSFile
     */
    private $riakGridFSFile;

    /**
     * Path to a file that is/was pending persistence.
     *
     * @var string
     */
    private $filename;

    /**
     * Bytes that are/were pending persistence.
     *
     * @var string
     */
    private $bytes;

    /**
     * Whether or not the file is dirty (i.e in need of persistence).
     *
     * @var string
     */
    private $isDirty = false;

    /**
     * Constructor.
     *
     * If the $file parameter is a RiakGridFSFile instance, this file will not
     * initially be marked as dirty (i.e. in need of persistence).
     *
     * @param string|\RiakGridFSFile $file String filename or a GridFSFile object
     */
    public function __construct($file = null)
    {
        if ($file instanceof \RiakGridFSFile) {
            $this->riakGridFSFile = $file;
            $this->isDirty = false;
        } elseif (is_string($file)) {
            $this->filename = $file;
            $this->isDirty = true;
        }
    }

    /**
     * Get the bytes for this file.
     *
     * @return string|null
     */
    public function getBytes()
    {
        if ($this->isDirty && $this->bytes) {
            return $this->bytes;
        }
        if ($this->isDirty && $this->filename) {
            return file_get_contents($this->filename);
        }
        if ($this->riakGridFSFile instanceof \RiakGridFSFile) {
            return $this->riakGridFSFile->getBytes();
        }
        return null;
    }

    /**
     * Set the bytes to be persisted and mark the file as dirty.
     *
     * @param string $bytes
     */
    public function setBytes($bytes)
    {
        $this->bytes = $bytes;
        $this->isDirty = true;
    }

    /**
     * Get the filename for this file.
     *
     * @return string|null
     */
    public function getFilename()
    {
        if ($this->isDirty && $this->filename) {
            return $this->filename;
        }

        if ($this->riakGridFSFile instanceof \RiakGridFSFile && $filename = $this->riakGridFSFile->getFilename()) {
            return $filename;
        }

        return $this->filename;
    }

    /**
     * Set the filename to be persisted and mark the file as dirty.
     *
     * @param string $filename
     */
    public function setFilename($filename)
    {
        $this->filename = $filename;
        $this->isDirty = true;
    }

    /**
     * Get the PHP RiakGridFSFile instance being wrapped.
     *
     * @return \RiakGridFSFile
     */
    public function getRiakGridFSFile()
    {
        return $this->riakGridFSFile;
    }

    /**
     * Set the PHP RiakGridFSFile instance to wrap and mark the file as clean.
     *
     * @param \RiakGridFSFile $riakGridFSFile
     */
    public function setRiakGridFSFile(\RiakGridFSFile $riakGridFSFile)
    {
        $this->riakGridFSFile = $riakGridFSFile;
        $this->isDirty = false;
    }

    /**
     * Get the size of this file.
     *
     * @return integer
     */
    public function getSize()
    {
        if ($this->isDirty && $this->bytes) {
            return strlen($this->bytes);
        }
        if ($this->isDirty && $this->filename) {
            return filesize($this->filename);
        }
        if ($this->riakGridFSFile instanceof \RiakGridFSFile) {
            return $this->riakGridFSFile->getSize();
        }
        return 0;
    }

    /**
     * Check whether there are unpersisted bytes.
     *
     * @return boolean
     */
    public function hasUnpersistedBytes()
    {
        return ($this->isDirty && $this->bytes);
    }

    /**
     * Check whether there is an unpersisted file.
     *
     * @return boolean
     */
    public function hasUnpersistedFile()
    {
        return ($this->isDirty && $this->filename);
    }

    /**
     * Check whether the file is dirty.
     *
     * If $isDirty is not null, the dirty state will be set before its new value
     * is returned.
     *
     * @param boolean $isDirty
     * @return boolean
     */
    public function isDirty($isDirty = null)
    {
        if ($isDirty !== null) {
            $this->isDirty = (boolean) $isDirty;
        }
        return $this->isDirty;
    }

    /**
     * Writes this file to the path indicated by $filename.
     *
     * @param string $filename
     * @return integer Number of bytes written
     * @throws \BadMethodCallException if nothing can be written
     */
    public function write($filename)
    {
        if ($this->isDirty && $this->bytes) {
            return file_put_contents($filename, $this->bytes);
        }
        if ($this->isDirty && $this->filename) {
            return copy($this->filename, $filename);
        }
        if ($this->riakGridFSFile instanceof \RiakGridFSFile) {
            return $this->riakGridFSFile->write($filename);
        }
        throw new \BadMethodCallException('Nothing to write(). File is not persisted yet and is not dirty.');
    }
}
