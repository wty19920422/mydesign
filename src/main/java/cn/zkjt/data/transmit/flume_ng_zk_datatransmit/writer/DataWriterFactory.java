package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.process.TypeMap;

public class DataWriterFactory implements DataWriterFactoryInterface{
	
	private String classPath;
	
	public DataWriterFactory() {
//		String simpleName = getClass().getSimpleName();
//		String totalName = getClass().getName();
		String packageName = getClass().getPackage().getName();
		classPath = packageName.replace("writer", "datawriter");
//		classPath = totalName.replace(simpleName, "");		
	}
	
	public DataWriter createWriter(String typeName) {
		DataWriter dataWriter = null;
		TypeMap typeMap = new TypeMap();
		String className = typeMap.getClassName(typeName);
		String factoryClassName = classPath + "." + typeName + "." + className;
		try {
			Class<?> clazz = Class.forName(factoryClassName);
			dataWriter = (DataWriter) clazz.newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataWriter;
	}
}
