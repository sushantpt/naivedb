using MessagePack;
using MessagePack.Resolvers;

namespace naivedb.core.utils
{
    public static class MessagePackSerializerHelper
    {
        public static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard.WithResolver(
            CompositeResolver.Create(StandardResolverAllowPrivate.Instance, ContractlessStandardResolver.Instance));
    }
}