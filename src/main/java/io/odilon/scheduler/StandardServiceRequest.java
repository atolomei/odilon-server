package io.odilon.scheduler;

public interface StandardServiceRequest extends ServiceRequest {

	public String getUUID();
	public boolean isObjectOperation();
}
