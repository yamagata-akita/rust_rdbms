
pub const PAGE_SIZE: usize = 4096;

pub sturct PageId(pub u64);

pub struct DiskManager {
    // ヒープファイルのファイルディスクリプタ
    heap_file: File,
    // 採番するページIDを決めるカウンタ
    next_page_id: u64,
}


impl DiskManager {

    // コンストラクタ
    pub fn new(heap_file: File) -> io::Result<Self> {
        // ファイルサイズを取得
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;

        Ok(Self { heap_file, next_page_id })
    }

    // ファイルパスを指定して開く
    // ? -> エラーが帰ってきたらreturnする
    pub fn open(heap_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(heap_file_path)?;
        Self::new(heap_file);
    }

    // 新しいページIDを採番する
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }

    // ページのデータを読み出す
    pub fn read_page_data(&mut self, page_Id: PageId, data: &mut [u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();

        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;

        // データを書き込む
        self.heap_file.read_exact(data)
    }

    // データをページに書き出す
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();

        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;

        // データを書き込む
        self.heap_file.write_all(data)
    }
}

pub sturct PageId(pub u64);

pub struct DiskManager {
    // ヒープファイルのファイルディスクリプタ
    heap_file: File,
    // 採番するページIDを決めるカウンタ
    next_page_id: u64,
}


impl DiskManager {

    // コンストラクタ
    pub fn new(heap_file: File) -> io::Result<Self> {
        // ファイルサイズを取得
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;

        Ok(Self { heap_file, next_page_id })
    }

    // ファイルパスを指定して開く
    // ? -> エラーが帰ってきたらreturnする
    pub fn open(heap_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(heap_file_path)?;
        Self::new(heap_file);
    }

    // 新しいページIDを採番する
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }

    // ページのデータを読み出す
    pub fn read_page_data(&mut self, page_Id: PageId, data: &mut [u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();

        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;

        // データを書き込む
        self.heap_file.read_exact(data)
    }

    // データをページに書き出す
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();

        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;

        // データを書き込む
        self.heap_file.write_all(data)
    }
}