using System.Security.Cryptography;

namespace naivedb.core.utils
{
    /// <summary>
    /// Provides utility methods for computing checksum values.
    /// </summary>
    public static class ChecksumUtils
    {
        /// <summary>
        /// Computes the SHA256-based checksum for the given input data and returns the first 16 characters of the resulting hash as a string.
        /// </summary>
        /// <param name="data">The input data for which the checksum will be computed.</param>
        /// <returns>A 16-character string representing the computed checksum.</returns>
        public static string ComputeChecksum(byte[] data)
        {
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(data);
            return BitConverter.ToString(hash).Replace("-", "").Substring(0, 16);
        }
        
        /// <summary>
        /// Computes the CRC32C checksum of the specified data.
        /// </summary>
        /// <param name="data">The data to compute the checksum for.</param>
        /// <returns>The computed checksum.</returns>
        public static string ComputeCrc32C(byte[] data)
        {
            ArgumentNullException.ThrowIfNull(data);
            using var algo = new Crc32CImplementation();
            var hash = algo.ComputeHash(data);
            return ConvertToHex(hash);
        }
        
        private static string ConvertToHex(byte[] hash) => BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();

        /// <summary>
        /// Implements a CRC32C (Cyclic Redundancy Check) hash algorithm for computing checksums.
        /// </summary>
        private sealed class Crc32CImplementation : HashAlgorithm
        {
            private uint _crc = 0xFFFFFFFF;
            private static readonly uint[] Table = GenerateLookup();

            public override void Initialize() => _crc = 0xFFFFFFFF;
            public override int HashSize => 32;

            protected override void HashCore(byte[] array, int ibStart, int cbSize)
            {
                for (int i = ibStart; i < ibStart + cbSize; i++)
                    _crc = (_crc >> 8) ^ Table[(byte)_crc ^ array[i]];
            }

            protected override byte[] HashFinal()
            {
                _crc ^= 0xFFFFFFFF;
                var result = BitConverter.GetBytes(_crc);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(result);
                return result;
            }

            private static uint[] GenerateLookup()
            {
                const uint poly = 0x1EDC6F41;
                var table = new uint[256];
                for (uint i = 0; i < 256; i++)
                {
                    uint crc = i;
                    for (int j = 0; j < 8; j++)
                        crc = (crc >> 1) ^ ((crc & 1) != 0 ? poly : 0);
                    table[i] = crc;
                }
                return table;
            }
        }
    }
}