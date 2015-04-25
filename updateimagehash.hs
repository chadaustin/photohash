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

main :: IO ()
main = do
    let hashfile = "imagehash.txt"
    originalHashes <- readHashes hashfile

    -- update hashes
    

    writeHashes hashfile hashes
   
    path_to_hashes = dict((h[1], h[0]) for h in hashes)

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
