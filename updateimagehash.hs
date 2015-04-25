excludedFiles = [
    "Picasa.ini",
    "Thumbs.db",
    ".DS_Store",
    "imagehash.txt" ]

readHashes :: FilePath -> IO [(FilePath, ByteString)]

writeHashes :: FilePath -> [(FilePath, ByteString)] -> IO ()
writeHashes path hashes = do
    withFile (path ++ ".new") WriteMode $ \handle -> do
        forM_ hashes $ \(path, hash) ->
            hPutStrLn handle $ (BS.unpack hash) ++ " *./" ++ path

    renameFile (path ++ ".new") path

updateHashes :: FilePath -> Map.Map FilePath ByteString -> IO (Map.Map FilePath ByteString)
updateHashes root original = do
  fileQueue <- newTMQueueIO
  resultQueue <- pipeline fileQueue hasher
  
  -- find unknown hashes and insert into job queue
  walk root $ \(dirpath, dirnames, filenames) -> do
    forM_ (map (combine dirpath) filenames) $ \fullPath ->
      when (Map.member fullPath original) $ do
        atomically $ writeTMQueue fileQueue fullPath
  atomically $ closeTMQueue fileQueue

  -- remove hashes not in map
  
  -- consume results and update hash map
  hashes <- newIORef original
  processQueue resultQueue $ \(path, hash) -> do
    modifyIORef hashes $ Map.insert path hash
        
  readIORef hashes

main :: IO ()
main = do
    let hashfile = "imagehash.txt"
    originalHashes <- fmap Map.fromList $ readHashes hashfile

    hashes <- updateHashes '.' originalHashes

    writeHashes hashfile $ Map.toList hashes
   
    original_paths = set(path_to_hashes.keys())
    current_paths = set()

    sys.stdout.write('scanning: ')
    for dirpath, dirnames, filenames in os.walk('.'):
        filenames = [
            f for f in filenames
            if f not in FILTERED and not f.startswith('._')]
        current_paths.update(
            os.path.normpath(os.path.join(dirpath, fn))
            for fn in filenames)
        sys.stdout.write('.')
        sys.stdout.flush()
    print

    for path in sorted(current_paths - original_paths):
        added_hash = imagehash.gethasher(path)(path)
        print 'added:', path, added_hash
        path_to_hashes[path] = added_hash

    for path in sorted(original_paths - current_paths):
        print 'removed:', path
        del path_to_hashes[path]

if __name__ == '__main__':
    main()
