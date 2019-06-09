#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <chrono>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

#define LINE_SIZE (sizeof(char**))

#define DELIMITER_CHAR '\n'
#define FIXED_LEN 0

#if FIXED_LEN == 0
    #define DELIMITER_LEN 1
#else
    #define DELIMITER_LEN 0
#endif

unsigned long long COMPARE_STRINGS_COUNT = 0;

int compareStrings(const char* a, const char* b)
{
    COMPARE_STRINGS_COUNT++;

    return (FIXED_LEN == 0 ? strcmp(a, b) : memcmp(a, b, FIXED_LEN));
}

void getRun(char** up, char** down, unsigned long left, unsigned long &right, unsigned long len)
{
    int last_index = len - 1;
    if (right >= last_index) {
        return;
    }

    right++;
    if (compareStrings(up[right - 1], up[right]) <= 0) {
        down[right] = up[right];
        right++;

        while (right <= last_index && compareStrings(up[right - 1], up[right]) <= 0) {
            down[right] = up[right];
            right++;
        }
        right--;
    } else {
        down[right] = up[right];
        right++;

        while (right <= last_index && compareStrings(up[right - 1], up[right]) >= 0) {
            down[right] = up[right];
            right++;
        }
        right--;

        for (int i = 0; i < (right + 1 - left) / 2; i++) {
            char* tmp = down[left + i];
            down[left + i] = down[right - i];
            down[right - i] = tmp;
        }
    }
}

char** mergeSort(char** up, char** down, unsigned long left, unsigned long &right, unsigned long len)
{
    if (left == right) {
        down[left] = up[left];
        getRun(up, down, left, right, len);

        return down;
    }

    unsigned long middle = (left + right) / 2;

    char** l_buff = mergeSort(up, down, left, middle, len);
    if (middle >= right) {
        right = middle;
        return l_buff;
    }

    char** r_buff = mergeSort(up, down, middle + 1, right, len);

    char** target = l_buff == up ? down : up;

    if (right - left + 1 >= 1000000) {
        printf("mergeSort: %lu %lu        \r", left, right);
    }
    unsigned long l_cur = left, r_cur = middle + 1;
    for (unsigned long i = left; i <= right; i++) {
        if (l_cur <= middle && r_cur <= right) {
            if (compareStrings(l_buff[l_cur], r_buff[r_cur]) <= 0) {
                target[i] = l_buff[l_cur];
                l_cur++;
            } else {
                target[i] = r_buff[r_cur];
                r_cur++;
            }
        } else if (l_cur <= middle) {
            target[i] = l_buff[l_cur];
            l_cur++;
        } else {
            target[i] = r_buff[r_cur];
            r_cur++;
        }
    }

    return target;
}


unsigned long long READ_TIME = 0;
unsigned long long WRITE_TIME = 0;
unsigned long long SORT_TIME = 0;

typedef std::chrono::time_point<std::chrono::steady_clock> timepoint;
timepoint startTime()
{
    return std::chrono::steady_clock::now();
}

unsigned long long diffTime(timepoint start)
{
    timepoint finish = std::chrono::steady_clock::now();
    unsigned long long ms = std::chrono::duration_cast< std::chrono::milliseconds >(finish - start).count();
    return ms;
}


class FileBuffer
{
public:
    FILE* file;
    unsigned long long currentPos;
    unsigned long long eofPos;

    char* buffer;
    unsigned long bufferSize;
    unsigned long bufferLen;

    unsigned long dataPos;

    char** lines;
    char** currentLine;
    unsigned long lineCount;


    void init(FILE* file, char* buffer, unsigned long bufferSize)
    {
        this->file = file;

        unsigned int roundUp = (LINE_SIZE - ((unsigned long)buffer) % LINE_SIZE) % LINE_SIZE;
        this->buffer = buffer + roundUp;
        bufferSize -= roundUp;

        unsigned int roundDown = bufferSize % LINE_SIZE;
        this->bufferSize = bufferSize - roundDown;

        this->reset(false);
    }

    void reset(bool resetFile)
    {
        this->bufferLen = 0;
        this->dataPos = 0;

        this->currentPos = 0;
        this->eofPos = 0;

        this->lines = (char**)(this->buffer + this->bufferSize);
        this->lineCount = 0;
        this->currentLine = NULL;

        if (resetFile) {
            fseeko(this->file, 0, SEEK_SET);
        }
    }

    void setFileBounds(unsigned long startPos, unsigned long eofPos)
    {
        this->currentPos = startPos;
        this->eofPos = eofPos;
    }

    void freeSpace()
    {
        unsigned long rest = this->bufferLen - this->dataPos;

        memmove(this->buffer, this->buffer + this->dataPos, rest);

        this->dataPos = 0;
        this->bufferLen = rest;

        this->lines = (char**)(this->buffer + this->bufferSize);
        this->lineCount = 0;
        this->currentLine = NULL;
    }

    void fillBuffer(bool keepFreeSpace)
    {
        this->freeSpace();

        if (ftello(this->file) != this->currentPos) {
            fseeko(this->file, this->currentPos, SEEK_SET);
        }
        fseeko(this->file, this->currentPos, SEEK_SET);

        unsigned int multiplier = (keepFreeSpace ? 2 : 1);

        char* p = this->buffer;
        char* pPrev = this->buffer;
        unsigned long availableBytes = this->bufferSize - this->bufferLen - (this->lineCount * LINE_SIZE * multiplier);

        if (this->bufferLen > 0) {
            setupLines(p, pPrev, availableBytes, multiplier);
        }

        bool isEof = false;

        if (availableBytes > 0) {
            while (true) {
                unsigned long spaceForData = MIN(availableBytes / 2, availableBytes - 1 * LINE_SIZE * multiplier);

                if (availableBytes - spaceForData < 1 * LINE_SIZE * multiplier || spaceForData <= DELIMITER_LEN) break;

                unsigned long bytesForRead = spaceForData - DELIMITER_LEN;  // for last newline if needed
                unsigned long restToEof = (this->eofPos > 0 ? this->eofPos - this->currentPos : bytesForRead);
                bytesForRead = MIN(bytesForRead, restToEof);

                timepoint st = startTime();
                unsigned long bytesRead = fread(this->buffer + this->bufferLen, 1, bytesForRead, this->file);
                READ_TIME += diffTime(st);

                this->currentPos += bytesRead;
                this->bufferLen += bytesRead;
                availableBytes -= bytesRead;

                if (bytesRead == 0) {
                    isEof = true;

                    if (DELIMITER_LEN != 0 && this->bufferLen > 0 && this->buffer[this->bufferLen - 1] != DELIMITER_CHAR && this->buffer[this->bufferLen - 1] != '\0') {
                        this->buffer[this->bufferLen] = DELIMITER_CHAR;
                        this->currentPos++;
                        this->bufferLen++;
                        if (this->eofPos > 0) this->eofPos++;

                        availableBytes--;
                    }
                }

                setupLines(p, pPrev, availableBytes, multiplier);

                if (isEof) break;
            }
        }

        this->reverseLines();
        this->currentLine = this->lines;
        this->dataPos = pPrev - this->buffer;

        if (this->lineCount == 0 && !isEof) {
            printf("\nline is too long: pos: %llu, buffer size: %lu\n", ftello(this->file) - this->bufferLen, this->bufferSize);
            if (this->bufferLen > 0) {
                this->buffer[this->bufferLen - 1] = '\0';
                printf("%s\n", this->buffer);
            }
            exit(1);
        }
    }

    void setupLines(char* &p, char* &pPrev, unsigned long &availableBytes, unsigned int multiplier)
    {
        while (true) {
            pPrev = p;

            if (availableBytes < LINE_SIZE * multiplier) break;

            unsigned long rest = this->bufferLen - (pPrev - this->buffer);
            char* pNext = (FIXED_LEN == 0 ? (char*)memchr(p, DELIMITER_CHAR, rest) : (FIXED_LEN <= rest ? p + FIXED_LEN : NULL));
            if (pNext == NULL) break;

            p = pNext;

            this->lines--;
            *this->lines = pPrev;
            this->lineCount++;

            availableBytes -= LINE_SIZE * multiplier;

            if (FIXED_LEN == 0) {
                *p = '\0';
                p++;
            }
        }
    }

    void reverseLines()
    {
        if (this->lineCount == 0) return;

        char** left = this->lines;
        char** right = this->lines + this->lineCount - 1;

        char* temp;
        for (unsigned long i = 0; i < this->lineCount / 2; i++) {
            temp = *left; *left = *right; *right = temp;

            left++;
            right--;
        }
    }

    void read(char* buf, unsigned long size)
    {
        if (this->dataPos >= this->bufferLen) {
            this->bufferLen = 0;
            this->dataPos = 0;

            timepoint st = startTime();
            unsigned long bytesRead = fread(this->buffer, 1, this->bufferSize, this->file);
            READ_TIME += diffTime(st);

            this->currentPos += bytesRead;
            this->bufferLen += bytesRead;

            if (bytesRead == 0) {
                return;
            }
        }

        char* p = this->buffer + this->dataPos;
        memcpy(buf, p, size);
        this->dataPos += size;
    }

    void write(char* buf, unsigned long size)
    {
        while (size > 0) {
            char* p = this->buffer + this->bufferLen;
            unsigned long nCur = MIN(size, this->bufferSize - this->bufferLen);

            memcpy(p, buf, nCur);
            this->currentPos += nCur;
            this->bufferLen += nCur;
            buf += nCur;
            size -= nCur;

            if (this->bufferLen == this->bufferSize) {
                this->flush();
            }
        }
    }

    void flush()
    {
        timepoint st = startTime();
        fwrite(this->buffer, 1, this->bufferLen, this->file);
        fflush(this->file);
        WRITE_TIME += diffTime(st);

        this->bufferLen = 0;
    }

    void writeLine(char* ln)
    {
        if (FIXED_LEN == 0) {
            this->write(ln, strlen(ln));
            const char c = DELIMITER_CHAR;
            this->write((char*)&c, 1);
        } else {
            this->write(ln, FIXED_LEN);
        }
    }

    void writeLines(char** lines, unsigned long count)
    {
        for (unsigned long i = 0; i < count; ++i, ++lines) {
            this->writeLine(*lines);
        }
    }
};


char** sortChunk(FileBuffer* fbInput)
{
    fbInput->fillBuffer(true);

    char** ptr = fbInput->lines;
    if (fbInput->lineCount > 0) {
        timepoint st = startTime();
        unsigned long right = fbInput->lineCount - 1;
        ptr = mergeSort(fbInput->lines, fbInput->lines - fbInput->lineCount, 0, right, fbInput->lineCount);
        SORT_TIME += diffTime(st);
    }

    return ptr;
}

int binarySearchPos(FileBuffer** fbChunks, unsigned int cnt, char* str)
{
    unsigned int left = 0, right = cnt, middle = 0;
    do {
        middle = left + (right - left) / 2;

        if (compareStrings(*fbChunks[middle]->currentLine, str) < 0) {
            left = middle + 1;
        } else {
            right = middle;
        }
    } while (left < right);

    return left;
}

void placeAtPos(FileBuffer** fbChunks, int from, int to)
{
    if (from != to) {
        FileBuffer* tmp = fbChunks[from];
        for (int i = from; i < to; i++) {
            fbChunks[i] = fbChunks[i + 1];
        }
        fbChunks[to] = tmp;
    }
}

void mergeChunks(FileBuffer** fbChunks, unsigned int cnt, FileBuffer* fbOut)
{
    int i = 0, j = 0, pos = 0;

    for (i = cnt - 1; i >= 0; i--) {
        fbChunks[i]->fillBuffer(false);
        if (fbChunks[i]->lineCount == 0) {
            placeAtPos(fbChunks, i, cnt - 1);
            cnt--;
        }
    }

    for (i = cnt - 2; i >= 0; i--) {
        // pos should be increased by 1 but then elements should be moved back by 1 so nothing to change
        pos = binarySearchPos(fbChunks + i + 1, cnt - (i + 1), *fbChunks[i]->currentLine);
        pos += i;
        placeAtPos(fbChunks, i, pos);
    }

    while (cnt > 0) {
        fbOut->writeLine(*fbChunks[0]->currentLine);
        fbChunks[0]->currentLine++;

        if (fbChunks[0]->currentLine >= fbChunks[0]->lines + fbChunks[0]->lineCount) {
            fbChunks[0]->fillBuffer(false);

            if (fbChunks[0]->lineCount == 0) {
                placeAtPos(fbChunks, 0, cnt - 1);
                cnt--;
                continue;
            }
        }

        if (cnt > 1) {
            pos = binarySearchPos(fbChunks + 1, cnt - 1, *fbChunks[0]->currentLine);
            placeAtPos(fbChunks, 0, pos);
        }
    }
}


FileBuffer* fbInput;
FileBuffer* fbTmp1; FileBuffer* fbTmpChunkPos1;
FileBuffer* fbTmp2; FileBuffer* fbTmpChunkPos2;
FileBuffer* fbChunksIn[64];
unsigned int chunksInCount;

unsigned int mergeAllChunks(
    FileBuffer* fbIn, FileBuffer* fbChunkPosIn,
    FileBuffer* fbOut, FileBuffer* fbChunkPosOut,
    unsigned int chunkCount, unsigned int mergeLevel
)
{
    unsigned long totalChunkCount = chunkCount, currentChunk = 0;

    unsigned long newCurrentChunkPos = 0, newChunkCount = 0;
    unsigned long currentChunkPos = 0, nextChunkPos = 0;

    fbOut->reset(true);
    fbChunkPosOut->reset(true);
    fbChunkPosIn->reset(true);
    fbChunkPosIn->setFileBounds(0, chunkCount * sizeof(currentChunkPos));

    fbChunkPosIn->read((char*)&nextChunkPos, sizeof(nextChunkPos));
    while (chunkCount > 0) {

        unsigned int loadedChunksInCount = MIN(chunksInCount, chunkCount);
        unsigned long chunkBufferSize = fbInput->bufferSize / loadedChunksInCount;

        for (unsigned int i = 0; i < loadedChunksInCount; i++) {
            currentChunkPos = nextChunkPos;
            fbChunkPosIn->read((char*)&nextChunkPos, sizeof(nextChunkPos));
            currentChunk++;

            // reuse input buffer
            char* buffer = fbInput->buffer + chunkBufferSize * i;

            fbChunksIn[i]->init(fbIn->file, buffer, chunkBufferSize);
            fbChunksIn[i]->setFileBounds(currentChunkPos, nextChunkPos);
        }

        printf("merge level: %u, chunks: %lu - %lu / %lu               \r",
            mergeLevel, currentChunk - loadedChunksInCount + 1, currentChunk, totalChunkCount
        );


        mergeChunks(fbChunksIn, loadedChunksInCount, fbOut);
        chunkCount -= loadedChunksInCount;


        fbChunkPosOut->write((char*)&newCurrentChunkPos, sizeof(newCurrentChunkPos));
        newChunkCount++;

        newCurrentChunkPos = fbOut->currentPos;
    }

    fbChunkPosOut->write((char*)&newCurrentChunkPos, sizeof(newCurrentChunkPos));

    fbOut->flush();
    fbChunkPosOut->flush();

    return newChunkCount;
}

FileBuffer* mergeSortFile()
{
    FileBuffer* fbOut = fbTmp1;
    FileBuffer* fbChunkPosOut = fbTmpChunkPos1;

    unsigned long currentChunkPos = 0, chunkCount = 0;
    unsigned long totalLineCount = 0;
    printf("lines: %lu                  \r", totalLineCount);

    while (true) {
        char** ptr = sortChunk(fbInput);
        if (fbInput->lineCount == 0) break;

        fbOut->writeLines(ptr, fbInput->lineCount);

        fbChunkPosOut->write((char*)&currentChunkPos, sizeof(currentChunkPos));
        chunkCount++;
        currentChunkPos += fbInput->dataPos;

        totalLineCount += fbInput->lineCount;
        printf("lines: %lu (%lu)        \r", totalLineCount, fbInput->lineCount);
    }

    fbChunkPosOut->write((char*)&currentChunkPos, sizeof(currentChunkPos));

    printf("lines: %lu                  \n", totalLineCount);


    if (chunkCount > 1) {
        fbOut->flush();
        fbChunkPosOut->flush();

        // will reuse input buffer
        fbInput->freeSpace();


        printf("sorted chunks for merge: %lu           \n", chunkCount);
        printf("merge chunks at once: %u               \n", chunksInCount);

        FileBuffer* fbIn = fbTmp2;
        FileBuffer* fbChunkPosIn = fbTmpChunkPos2;

        FileBuffer* tmp;
        unsigned int mergeLevel = 0;
        while (chunkCount > 1) {
            mergeLevel++;

            tmp = fbIn; fbIn = fbOut; fbOut = tmp;
            tmp = fbChunkPosIn; fbChunkPosIn = fbChunkPosOut; fbChunkPosOut = tmp;

            chunkCount = mergeAllChunks(
                fbIn, fbChunkPosIn, fbOut, fbChunkPosOut,
                chunkCount, mergeLevel
            );
        }

        printf("merge levels: %u                                                \n", mergeLevel);
    } else {
        printf("sorted in memory\n");
    }

    fbOut->flush();

    return fbOut;
}

void allocateMemory(unsigned long requiredSize, char* &buffer, unsigned long &bufferSize)
{
    buffer = (char*)malloc(requiredSize);

    while (buffer == NULL && requiredSize >= 32L) {
        requiredSize = requiredSize / 8 * 7;
        buffer = (char*)malloc(requiredSize);
    }
    if (buffer == NULL) {
        printf("cannot allocate memory buffer\n");
        exit(1);
    }

    bufferSize = requiredSize;
}

FILE* openTmpFile(char* nameBuf)
{
    sprintf(nameBuf, "sort%04u.txt", rand() % 10000);
    FILE* fp = fopen(nameBuf, "wb+");
    if (fp == NULL) {
        printf("open file error: %s\n", strerror(errno));
        exit(1);
    }

    return fp;
}

void runMergeSortFile(char* inputName, char* outputName, unsigned int mergeAtOnceCount, unsigned long certainBufferSize)
{
    timepoint st = startTime();

    FILE* input = fopen(inputName, "rb");

    if (input == NULL) {
        printf("open file error: %s\n", strerror(errno));
        exit(1);
    }

    remove(outputName);

    srand(time(NULL));
    setvbuf(stdout, NULL, _IONBF, 0);

    char tmpName1[20], tmpName2[20];
    char tmpNameChunkPos1[20], tmpNameChunkPos2[20];

    FILE* tmp1 = openTmpFile(tmpName1);
    FILE* tmp2 = openTmpFile(tmpName2);
    FILE* tmpChunkPos1 = openTmpFile(tmpNameChunkPos1);
    FILE* tmpChunkPos2 = openTmpFile(tmpNameChunkPos2);


    fbInput = new FileBuffer();

    fbTmp1 = new FileBuffer();
    fbTmp2 = new FileBuffer();
    fbTmpChunkPos1 = new FileBuffer();
    fbTmpChunkPos2 = new FileBuffer();

    mergeAtOnceCount = MIN(MAX(mergeAtOnceCount, 2), 64);
    chunksInCount = mergeAtOnceCount;

    for (int i = 0; i < mergeAtOnceCount; i++) {
        fbChunksIn[i] = new FileBuffer();
        if (fbChunksIn[i] == NULL) {
            printf("cannot create chunk buffer: %d\n", i);
            exit(1);
        }
    }


    char* buffer = NULL;
    unsigned long bufferSize = 0;

    // they are for write only and not in same time, write buffer can be the same
    allocateMemory(64*1024, buffer, bufferSize);
    fbTmp1->init(tmp1, buffer, bufferSize);
    fbTmp2->init(tmp2, buffer, bufferSize);


    // they are for read and write at same time, write buffers should be different
    allocateMemory(2*1024, buffer, bufferSize);
    fbTmpChunkPos1->init(tmpChunkPos1, buffer, bufferSize);

    allocateMemory(2*1024, buffer, bufferSize);
    fbTmpChunkPos2->init(tmpChunkPos2, buffer, bufferSize);


    fseeko(input, 0, SEEK_END);
    unsigned long long fileSize = ftello(input);
    fseeko(input, 0, SEEK_SET);

    unsigned long long maxLines = (FIXED_LEN == 0 ? fileSize : fileSize / FIXED_LEN);
    unsigned long long requiredSize = (fileSize + DELIMITER_LEN + (maxLines * LINE_SIZE * 2));
    if (certainBufferSize != 0 && requiredSize > certainBufferSize) requiredSize = certainBufferSize;
    allocateMemory((requiredSize < SIZE_MAX ? requiredSize : SIZE_MAX), buffer, bufferSize);

    printf("file size: %llu\n", fileSize);
    printf("buffer size: %lu\n", bufferSize);

    fbInput->init(input, buffer, bufferSize);


    COMPARE_STRINGS_COUNT = 0;
    FileBuffer* fbOut = mergeSortFile();

    printf("comparisons: %llu\n", COMPARE_STRINGS_COUNT);
    printf("sort time:   %llu \nread time:   %llu \nwrite time:  %llu\n", SORT_TIME, READ_TIME, WRITE_TIME);


    fclose(tmp1); fclose(tmpChunkPos1);
    fclose(tmp2); fclose(tmpChunkPos2);

    if (fbOut != fbTmp1) remove(tmpName1);
    if (fbOut != fbTmp2) remove(tmpName2);
    remove(tmpNameChunkPos1);
    remove(tmpNameChunkPos2);

    free(fbTmp1->buffer);
    free(fbTmpChunkPos1->buffer);
    free(fbTmpChunkPos2->buffer);
    free(buffer);

    fclose(input);

    rename((fbOut == fbTmp1 ? tmpName1 : tmpName2), outputName);


    printf("total time:  %llu\n", diffTime(st));
    printf("\n");
}

unsigned long parseBufferSize(char* str)
{
    unsigned long bufferSize = atol(str);
    if (str[strlen(str) - 1] == 'k') bufferSize *= 1024L;
    if (str[strlen(str) - 1] == 'M') bufferSize *= 1024L * 1024;

    return bufferSize;
}

int main(int argc, char** argv)
{
    if (argc < 3) {
        printf("msf <input> <output> [<chunks count>] [<buffer size>]\n");
        printf("sort text file using merge sort\n");
        printf("input - input file\n");
        printf("input - output file\n");
        printf("buffer size - size of read buffer (default as much as possible)\n");
        printf("chunks count - count to merge at once (default 16)\n");
        return 0;
    }

    char* inputName = argv[1];
    char* outputName = argv[2];
    unsigned long certainBufferSize = (argc > 3 && argv[3][0] != '-' ? parseBufferSize(argv[3]) : 0);
    int mergeAtOnceCount = (argc > 4 ? atoi(argv[4]) : 16);

    runMergeSortFile(inputName, outputName, mergeAtOnceCount, certainBufferSize);

    return 0;
}
